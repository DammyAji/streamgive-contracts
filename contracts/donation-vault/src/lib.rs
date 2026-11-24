// SPDX-License-Identifier: Apache-2.0
#![no_std]

mod math;
pub mod events;
pub mod storage;
pub mod types;

use soroban_sdk::{contract, contractimpl, token, Address, Bytes, BytesN, Map, Vec, Env};

pub use events::*;
pub use types::*;
use storage::*;

fn create_stream_inner(
    env: Env,
    donor: Address,
    ngo: Address,
    token: Address,
    deposit: i128,
    rate: i128,
    start: Option<u64>,
    memo: Bytes,
) -> Result<u64, Error> {
    require_not_paused(&env)?;
    donor.require_auth();

    if donor == ngo {
        return Err(Error::SelfStream);
    }

    let vault = env.current_contract_address();
    if ngo == vault {
        return Err(Error::InvalidNgo);
    }
    if token == vault {
        return Err(Error::InvalidToken);
    }

    if deposit <= 0 || rate <= 0 {
        return Err(Error::InvalidAmount);
    }

    let now = env.ledger().timestamp();
    let start = start.unwrap_or(now);
    if start > now.saturating_add(MAX_START_DELAY_SECONDS) {
        return Err(Error::StartTooFar);
    }

    if memo.len() > MAX_MEMO_BYTES {
        return Err(Error::MemoTooLong);
    }

    if deposit < DonationVault::min_deposit(env.clone()) {
        return Err(Error::DepositTooLow);
    }

    if let Some(max_rate) = max_rate_cap(&env) {
        if rate > max_rate {
            return Err(Error::RateTooHigh);
        }
    }

    if depletes_before_min_duration(deposit, rate, min_duration_seconds(&env)) {
        return Err(Error::StreamTooShort);
    }

    let max_streams: u64 = env
        .storage()
        .instance()
        .get(&DataKey::MaxStreamsPerDonor)
        .unwrap_or(DEFAULT_MAX_STREAMS_PER_DONOR);
    let donor_streams: u64 = env
        .storage()
        .persistent()
        .get(&DataKey::DonorStreamCount(donor.clone()))
        .unwrap_or(0);
    if donor_streams >= max_streams {
        return Err(Error::StreamLimitExceeded);
    }

    require_verified_ngo(&env, &ngo)?;

    let total_donated: i128 = env
        .storage()
        .instance()
        .get(&DataKey::TotalDonated)
        .unwrap_or(0);
    let next_total = total_donated
        .checked_add(deposit)
        .ok_or(Error::ArithmeticOverflow)?;

    let stream_id: u64 = env
        .storage()
        .instance()
        .get(&DataKey::NextStreamId)
        .ok_or(Error::StreamCounterMissing)?;

    let stream = Stream {
        donor: donor.clone(),
        ngo: ngo.clone(),
        token: token.clone(),
        rate,
        balance: deposit,
        withdrawn: 0,
        created_at: now,
        start,
        last_update: now,
        memo: memo.clone(),
        status: StreamStatus::Active,
        cancelled: false,
    };

    env.storage()
        .persistent()
        .set(&DataKey::Stream(stream_id), &stream);
    let ngo_key = DataKey::NgoStreams(ngo.clone());
    let mut ngo_streams: Vec<u64> = env
        .storage()
        .persistent()
        .get(&ngo_key)
        .unwrap_or_else(|| Vec::new(&env));
    ngo_streams.push_back(stream_id);
    env.storage().persistent().set(&ngo_key, &ngo_streams);
    env.storage()
        .persistent()
        .extend_ttl(&ngo_key, STREAM_LIFETIME_THRESHOLD, STREAM_BUMP_AMOUNT);
    let next_stream_id = stream_id.checked_add(1).ok_or(Error::ArithmeticOverflow)?;
    env.storage()
        .instance()
        .set(&DataKey::NextStreamId, &next_stream_id);

    let donor_key = DataKey::DonorStreamCount(donor.clone());
    let new_count = donor_streams
        .checked_add(1)
        .ok_or(Error::ArithmeticOverflow)?;
    env.storage().persistent().set(&donor_key, &new_count);
    env.storage().persistent().extend_ttl(
        &donor_key,
        STREAM_LIFETIME_THRESHOLD,
        STREAM_BUMP_AMOUNT,
    );

    env.storage()
        .instance()
        .set(&DataKey::TotalDonated, &next_total);

    extend_instance_ttl(&env);
    extend_stream_ttl(&env, stream_id);

    add_committed(&env, &token, deposit)?;

    let token_client = token::Client::new(&env, &token);
    token_client.transfer(&donor, env.current_contract_address(), &deposit);

    CreatedEvent {
        stream_id,
        donor,
        ngo,
        token,
        deposit,
        rate,
        memo,
    }
    .publish(&env);

    Ok(stream_id)
}

#[contract]
pub struct DonationVault;

#[contractimpl]
impl DonationVault {
    pub fn __constructor(env: Env, admin: Address) {
        env.storage().instance().set(&DataKey::Admin, &admin);
        env.storage().instance().set(&DataKey::NextStreamId, &0u64);
        env.storage().instance().set(&DataKey::MinDeposit, &0i128);
        env.storage().instance().set(&DataKey::TotalDonated, &0i128);
        env.storage().instance().set(&DataKey::TotalWithdrawn, &0i128);
        env.storage().instance().set(&DataKey::TotalFeesCollected, &0i128);
        env.storage().instance().set(&DataKey::CancelGraceLedgers, &0u32);
        extend_instance_ttl(&env);
        InitializedEvent { admin }.publish(&env);
    }

    pub fn init(env: Env, admin: Address) -> Result<(), Error> {
        admin.require_auth();
        if admin_renounced(&env) {
            return Err(Error::AdminRenounced);
        }
        if env.storage().instance().has(&DataKey::Admin) {
            return Err(Error::AlreadyInitialized);
        }
        env.storage().instance().set(&DataKey::Admin, &admin);
        env.storage().instance().set(&DataKey::NextStreamId, &0u64);
        env.storage().instance().set(&DataKey::MinDeposit, &0i128);
        env.storage().instance().set(&DataKey::TotalDonated, &0i128);
        env.storage().instance().set(&DataKey::TotalWithdrawn, &0i128);
        env.storage().instance().set(&DataKey::TotalFeesCollected, &0i128);
        env.storage().instance().set(&DataKey::CancelGraceLedgers, &0u32);
        extend_instance_ttl(&env);
        InitializedEvent { admin }.publish(&env);
        Ok(())
    }

    pub fn renounce_admin(env: Env) -> Result<(), Error> {
        require_admin(&env)?;
        env.storage().instance().remove(&DataKey::Admin);
        env.storage().instance().remove(&DataKey::PendingAdmin);
        env.storage().instance().set(&DataKey::AdminRenounced, &true);
        extend_instance_ttl(&env);
        RenouncedAdminEvent { data: () }.publish(&env);
        Ok(())
    }

    pub fn total_donated(env: Env) -> i128 {
        env.storage().instance().get(&DataKey::TotalDonated).unwrap_or(0)
    }

    pub fn total_withdrawn(env: Env) -> i128 {
        env.storage().instance().get(&DataKey::TotalWithdrawn).unwrap_or(0)
    }

    pub fn total_fees_collected(env: Env) -> i128 {
        env.storage().instance().get(&DataKey::TotalFeesCollected).unwrap_or(0)
    }

    pub fn admin(env: Env) -> Result<Address, Error> {
        if admin_renounced(&env) {
            return Err(Error::AdminRenounced);
        }
        env.storage().instance().get(&DataKey::Admin).ok_or(Error::NotInitialized)
    }

    pub fn pending_admin(env: Env) -> Option<Address> {
        env.storage().instance().get(&DataKey::PendingAdmin)
    }

    pub fn propose_admin(env: Env, new_admin: Address) -> Result<(), Error> {
        let current_admin = require_admin(&env)?;
        if new_admin == current_admin {
            return Err(Error::InvalidAdmin);
        }
        env.storage().instance().set(&DataKey::PendingAdmin, &new_admin);
        extend_instance_ttl(&env);
        ProposedAdminEvent { admin: new_admin }.publish(&env);
        Ok(())
    }

    pub fn accept_admin(env: Env) -> Result<(), Error> {
        let pending: Address = env
            .storage()
            .instance()
            .get(&DataKey::PendingAdmin)
            .ok_or(Error::NoPendingAdmin)?;
        pending.require_auth();
        env.storage().instance().set(&DataKey::Admin, &pending);
        env.storage().instance().remove(&DataKey::PendingAdmin);
        extend_instance_ttl(&env);
        AcceptedAdminEvent { admin: pending }.publish(&env);
        Ok(())
    }

    pub fn cancel_admin_proposal(env: Env) -> Result<(), Error> {
        require_admin(&env)?;
        if !env.storage().instance().has(&DataKey::PendingAdmin) {
            return Err(Error::NoPendingAdmin);
        }
        env.storage().instance().remove(&DataKey::PendingAdmin);
        extend_instance_ttl(&env);
        CancelledAdminEvent { data: () }.publish(&env);
        Ok(())
    }

    pub fn get_stream(env: Env, stream_id: u64) -> Result<Stream, Error> {
        env.storage()
            .persistent()
            .get(&DataKey::Stream(stream_id))
            .ok_or(Error::StreamNotFound)
    }

    pub fn get_streams(env: Env, ids: Vec<u64>) -> Result<Vec<Option<Stream>>, Error> {
        if ids.len() > MAX_BATCH_GET_STREAMS {
            return Err(Error::BatchLimitExceeded);
        }
        let mut streams: Vec<Option<Stream>> = Vec::new(&env);
        for id in ids.iter() {
            streams.push_back(env.storage().persistent().get(&DataKey::Stream(id)));
        }
        Ok(streams)
    }

    pub fn streams_by_donor(env: Env, donor: Address) -> Vec<u64> {
        let total: u64 = env
            .storage()
            .instance()
            .get(&DataKey::NextStreamId)
            .unwrap_or(0);
        let mut ids = Vec::new(&env);
        for id in 0..total {
            if let Some(stream) = env
                .storage()
                .persistent()
                .get::<_, Stream>(&DataKey::Stream(id))
            {
                if stream.donor == donor {
                    ids.push_back(id);
                }
            }
        }
        ids
    }

    pub fn streams_by_ngo(env: Env, ngo: Address, start: u32, limit: u32) -> Vec<u64> {
        let ngo_streams: Vec<u64> = env
            .storage()
            .persistent()
            .get(&DataKey::NgoStreams(ngo))
            .unwrap_or_else(|| Vec::new(&env));
        let capped = limit.min(MAX_STREAMS_BY_NGO);
        let mut ids = Vec::new(&env);
        if start >= ngo_streams.len() {
            return ids;
        }
        let end = (start as u32).saturating_add(capped).min(ngo_streams.len());
        for i in start..end {
            if let Some(id) = ngo_streams.get(i) {
                ids.push_back(id);
            }
        }
        ids
    }

    pub fn stream_count(env: Env) -> u64 {
        env.storage().instance().get(&DataKey::NextStreamId).unwrap_or(0)
    }

    pub fn pending_accrual(env: Env, stream_id: u64) -> Result<i128, Error> {
        let stream: Stream = env
            .storage()
            .persistent()
            .get(&DataKey::Stream(stream_id))
            .ok_or(Error::StreamNotFound)?;
        let now = env.ledger().timestamp();
        let elapsed = now.saturating_sub(accrual_base(&stream));
        Ok(math::accrued(stream.rate, elapsed, stream.balance))
    }

    pub fn depletion_time(env: Env, stream_id: u64) -> Result<Option<u64>, Error> {
        let stream: Stream = env
            .storage()
            .persistent()
            .get(&DataKey::Stream(stream_id))
            .ok_or(Error::StreamNotFound)?;
        Ok(math::seconds_to_deplete(stream.rate, stream.balance)
            .and_then(|seconds| accrual_base(&stream).checked_add(seconds)))
    }

    pub fn pending_payout(env: Env, stream_id: u64) -> Result<(i128, i128), Error> {
        let stream: Stream = env
            .storage()
            .persistent()
            .get(&DataKey::Stream(stream_id))
            .ok_or(Error::StreamNotFound)?;
        let now = env.ledger().timestamp();
        let elapsed = now.saturating_sub(accrual_base(&stream));
        let gross = math::accrued(stream.rate, elapsed, stream.balance);
        let fee = compute_fee(&env, &stream.token, gross);
        Ok((gross - fee, fee))
    }

    pub fn pending_refund(env: Env, stream_id: u64) -> Result<i128, Error> {
        let stream: Stream = env
            .storage()
            .persistent()
            .get(&DataKey::Stream(stream_id))
            .ok_or(Error::StreamNotFound)?;
        let accrued = if stream.status == StreamStatus::Paused {
            0
        } else {
            let now = env.ledger().timestamp();
            let elapsed = now.saturating_sub(accrual_base(&stream));
            math::accrued(stream.rate, elapsed, stream.balance)
        };
        Ok(stream.balance - accrued)
    }

    pub fn extend_stream(env: Env, stream_id: u64) -> Result<(), Error> {
        if !env.storage().persistent().has(&DataKey::Stream(stream_id)) {
            return Err(Error::StreamNotFound);
        }
        extend_stream_ttl(&env, stream_id);
        Ok(())
    }

    pub fn pause(env: Env) -> Result<(), Error> {
        require_admin(&env)?;
        let already_paused = env
            .storage()
            .instance()
            .get::<DataKey, bool>(&DataKey::Paused)
            .unwrap_or(false);
        if already_paused {
            return Err(Error::AlreadyPaused);
        }
        env.storage().instance().set(&DataKey::Paused, &true);
        extend_instance_ttl(&env);
        PausedEvent { data: () }.publish(&env);
        Ok(())
    }

    pub fn unpause(env: Env) -> Result<(), Error> {
        require_admin(&env)?;
        let already_unpaused = !env
            .storage()
            .instance()
            .get::<DataKey, bool>(&DataKey::Paused)
            .unwrap_or(false);
        if already_unpaused {
            return Err(Error::AlreadyUnpaused);
        }
        env.storage().instance().set(&DataKey::Paused, &false);
        extend_instance_ttl(&env);
        UnpausedEvent { data: () }.publish(&env);
        Ok(())
    }

    pub fn paused(env: Env) -> bool {
        env.storage().instance().get(&DataKey::Paused).unwrap_or(false)
    }

    pub fn get_config(env: Env) -> Config {
        Config {
            paused: env.storage().instance().get(&DataKey::Paused).unwrap_or(false),
            treasury: env.storage().instance().get(&DataKey::Treasury),
            fee_bps: env.storage().instance().get(&DataKey::FeeBps).unwrap_or(0),
            min_deposit: env.storage().instance().get(&DataKey::MinDeposit).unwrap_or(0),
            max_streams_per_donor: env
                .storage()
                .instance()
                .get(&DataKey::MaxStreamsPerDonor)
                .unwrap_or(DEFAULT_MAX_STREAMS_PER_DONOR),
            max_rate: max_rate_cap(&env),
            min_duration: min_duration_seconds(&env),
            registry: env.storage().instance().get(&DataKey::Registry),
            cancel_grace_ledgers: env
                .storage()
                .instance()
                .get(&DataKey::CancelGraceLedgers)
                .unwrap_or(0),
            total_donated: env.storage().instance().get(&DataKey::TotalDonated).unwrap_or(0),
            total_withdrawn: env.storage().instance().get(&DataKey::TotalWithdrawn).unwrap_or(0),
            total_fees_collected: env
                .storage()
                .instance()
                .get(&DataKey::TotalFeesCollected)
                .unwrap_or(0),
            pending_admin: env.storage().instance().get(&DataKey::PendingAdmin),
            renounced: env.storage().instance().get(&DataKey::AdminRenounced).unwrap_or(false),
        }
    }

    pub fn set_treasury(env: Env, treasury: Address) -> Result<(), Error> {
        require_admin(&env)?;
        if treasury == env.current_contract_address() {
            return Err(Error::InvalidTreasury);
        }
        env.storage().instance().set(&DataKey::Treasury, &treasury);
        extend_instance_ttl(&env);
        TreasurySetEvent { treasury }.publish(&env);
        Ok(())
    }

    pub fn treasury(env: Env) -> Option<Address> {
        env.storage().instance().get(&DataKey::Treasury)
    }

    pub fn clear_treasury(env: Env) -> Result<(), Error> {
        require_admin(&env)?;
        let fee_bps: u32 = env.storage().instance().get(&DataKey::FeeBps).unwrap_or(0);
        if fee_bps > 0 {
            return Err(Error::FeeRequiresTreasury);
        }
        env.storage().instance().remove(&DataKey::Treasury);
        extend_instance_ttl(&env);
        TreasuryClearedEvent { data: () }.publish(&env);
        Ok(())
    }

    pub fn set_fee_bps(env: Env, fee_bps: u32) -> Result<(), Error> {
        require_admin(&env)?;
        if fee_bps > MAX_FEE_BPS {
            return Err(Error::FeeTooHigh);
        }
        if fee_bps > 0 {
            let treasury: Option<Address> = env.storage().instance().get(&DataKey::Treasury);
            if treasury.is_none() {
                return Err(Error::FeeRequiresTreasury);
            }
        }
        env.storage().instance().set(&DataKey::FeeBps, &fee_bps);
        extend_instance_ttl(&env);
        FeeBpsSetEvent { fee_bps }.publish(&env);
        Ok(())
    }

    pub fn fee_bps(env: Env) -> u32 {
        env.storage().instance().get(&DataKey::FeeBps).unwrap_or(0)
    }

    pub fn max_fee_bps(_env: Env) -> u32 {
        MAX_FEE_BPS
    }

    pub fn set_token_fee_bps(env: Env, token: Address, fee_bps: u32) -> Result<(), Error> {
        require_admin(&env)?;
        if fee_bps > MAX_FEE_BPS {
            return Err(Error::FeeTooHigh);
        }
        if fee_bps > 0 {
            let treasury: Option<Address> = env.storage().instance().get(&DataKey::Treasury);
            if treasury.is_none() {
                return Err(Error::FeeRequiresTreasury);
            }
        }
        let key = DataKey::TokenFeeBps(token.clone());
        env.storage().persistent().set(&key, &fee_bps);
        env.storage().persistent().extend_ttl(&key, INSTANCE_LIFETIME_THRESHOLD, INSTANCE_BUMP_AMOUNT);
        extend_instance_ttl(&env);
        TokenFeeBpsSetEvent { token, fee_bps }.publish(&env);
        Ok(())
    }

    pub fn clear_token_fee_bps(env: Env, token: Address) -> Result<(), Error> {
        require_admin(&env)?;
        let key = DataKey::TokenFeeBps(token.clone());
        if !env.storage().persistent().has(&key) {
            return Ok(());
        }
        env.storage().persistent().remove(&key);
        extend_instance_ttl(&env);
        TokenFeeBpsClearedEvent { token, data: () }.publish(&env);
        Ok(())
    }

    pub fn token_fee_bps(env: Env, token: Address) -> u32 {
        effective_fee_bps(&env, &token)
    }

    pub fn allowed_tokens(env: Env) -> Vec<Address> {
        env.storage()
            .instance()
            .get(&DataKey::AllowedTokens)
            .unwrap_or_else(|| Vec::new(&env))
    }

    pub fn set_min_deposit(env: Env, min_deposit: i128) -> Result<(), Error> {
        require_admin(&env)?;
        if min_deposit < 0 {
            return Err(Error::InvalidAmount);
        }
        env.storage().instance().set(&DataKey::MinDeposit, &min_deposit);
        extend_instance_ttl(&env);
        MinDepositSetEvent { min_deposit }.publish(&env);
        Ok(())
    }

    pub fn min_deposit(env: Env) -> i128 {
        env.storage().instance().get(&DataKey::MinDeposit).unwrap_or(0)
    }

    pub fn set_cancel_grace_ledgers(env: Env, grace_ledgers: u32) -> Result<(), Error> {
        require_admin(&env)?;
        STREAM_BUMP_AMOUNT
            .checked_add(grace_ledgers)
            .ok_or(Error::ArithmeticOverflow)?;
        env.storage().instance().set(&DataKey::CancelGraceLedgers, &grace_ledgers);
        extend_instance_ttl(&env);
        CancelGraceLedgersSetEvent { grace_ledgers }.publish(&env);
        Ok(())
    }

    pub fn cancel_grace_ledgers(env: Env) -> u32 {
        env.storage().instance().get(&DataKey::CancelGraceLedgers).unwrap_or(0)
    }

    pub fn set_max_streams_per_donor(env: Env, limit: u64) -> Result<(), Error> {
        require_admin(&env)?;
        env.storage().instance().set(&DataKey::MaxStreamsPerDonor, &limit);
        extend_instance_ttl(&env);
        MaxStreamsPerDonorSetEvent { limit }.publish(&env);
        Ok(())
    }

    pub fn max_streams_per_donor(env: Env) -> u64 {
        env.storage()
            .instance()
            .get(&DataKey::MaxStreamsPerDonor)
            .unwrap_or(DEFAULT_MAX_STREAMS_PER_DONOR)
    }

    pub fn set_max_rate(env: Env, max_rate: i128) -> Result<(), Error> {
        require_admin(&env)?;
        if max_rate <= 0 {
            return Err(Error::InvalidAmount);
        }
        env.storage().instance().set(&DataKey::MaxRate, &max_rate);
        extend_instance_ttl(&env);
        MaxRateSetEvent { max_rate }.publish(&env);
        Ok(())
    }

    pub fn clear_max_rate(env: Env) -> Result<(), Error> {
        require_admin(&env)?;
        env.storage().instance().remove(&DataKey::MaxRate);
        extend_instance_ttl(&env);
        MaxRateClearedEvent { data: () }.publish(&env);
        Ok(())
    }

    pub fn max_rate(env: Env) -> Option<i128> {
        max_rate_cap(&env)
    }

    pub fn set_min_duration(env: Env, min_duration: u64) -> Result<(), Error> {
        require_admin(&env)?;
        env.storage().instance().set(&DataKey::MinDuration, &min_duration);
        extend_instance_ttl(&env);
        MinDurationSetEvent { min_duration }.publish(&env);
        Ok(())
    }

    pub fn min_duration(env: Env) -> u64 {
        min_duration_seconds(&env)
    }

    pub fn set_registry(env: Env, registry: Address) -> Result<(), Error> {
        require_admin(&env)?;
        env.storage().instance().set(&DataKey::Registry, &registry);
        extend_instance_ttl(&env);
        RegistrySetEvent { registry }.publish(&env);
        Ok(())
    }

    pub fn clear_registry(env: Env) -> Result<(), Error> {
        require_admin(&env)?;
        env.storage().instance().remove(&DataKey::Registry);
        extend_instance_ttl(&env);
        RegistryClearedEvent { data: () }.publish(&env);
        Ok(())
    }

    pub fn registry(env: Env) -> Option<Address> {
        env.storage().instance().get(&DataKey::Registry)
    }

    pub fn set_payout_address(env: Env, ngo: Address, payout_address: Address) -> Result<(), Error> {
        ngo.require_auth();
        let key = DataKey::NgoPayoutAddress(ngo.clone());
        env.storage().persistent().set(&key, &payout_address);
        env.storage().persistent().extend_ttl(&key, INSTANCE_LIFETIME_THRESHOLD, INSTANCE_BUMP_AMOUNT);
        extend_instance_ttl(&env);
        PayoutAddressSetEvent { ngo, payout: payout_address }.publish(&env);
        Ok(())
    }

    pub fn payout_address(env: Env, ngo: Address) -> Option<Address> {
        env.storage().persistent().get(&DataKey::NgoPayoutAddress(ngo))
    }

    pub fn create_stream(env: Env, donor: Address, ngo: Address, token: Address, deposit: i128, rate: i128) -> Result<u64, Error> {
        create_stream_inner(env.clone(), donor, ngo, token, deposit, rate, None, Bytes::new(&env))
    }

    pub fn create_stream_with_start(env: Env, donor: Address, ngo: Address, token: Address, deposit: i128, rate: i128, start: u64) -> Result<u64, Error> {
        create_stream_inner(env.clone(), donor, ngo, token, deposit, rate, Some(start), Bytes::new(&env))
    }

    pub fn create_stream_with_memo(env: Env, donor: Address, ngo: Address, token: Address, deposit: i128, rate: i128, memo: Bytes) -> Result<u64, Error> {
        create_stream_inner(env, donor, ngo, token, deposit, rate, None, memo)
    }

    pub fn pause_stream(env: Env, stream_id: u64) -> Result<i128, Error> {
        require_not_paused(&env)?;
        let key = DataKey::Stream(stream_id);
        let mut stream: Stream = env
            .storage()
            .persistent()
            .get(&key)
            .ok_or(Error::StreamNotFound)?;

        stream.donor.require_auth();
        if stream.cancelled {
            return Err(Error::StreamCancelled);
        }
        if stream.status == StreamStatus::Paused {
            return Err(Error::StreamAlreadyPaused);
        }
        if stream.status == StreamStatus::Drained {
            return Err(Error::StreamCancelled);
        }
        if stream.balance == 0 {
            return Err(Error::StreamCancelled);
        }

        let now = env.ledger().timestamp();
        let settled = settle(&env, stream_id, &mut stream, now)?;
        if stream.balance == 0 {
            stream.status = StreamStatus::Drained;
        } else {
            stream.status = StreamStatus::Paused;
        }
        env.storage().persistent().set(&key, &stream);
        extend_instance_ttl(&env);
        extend_stream_ttl(&env, stream_id);
        PausedStreamEvent { stream_id, settled }.publish(&env);
        Ok(settled)
    }

    pub fn resume_stream(env: Env, stream_id: u64) -> Result<(), Error> {
        require_not_paused(&env)?;
        let key = DataKey::Stream(stream_id);
        let mut stream: Stream = env
            .storage()
            .persistent()
            .get(&key)
            .ok_or(Error::StreamNotFound)?;

        stream.donor.require_auth();
        if stream.cancelled {
            return Err(Error::StreamCancelled);
        }
        if stream.status != StreamStatus::Paused {
            return Err(Error::StreamNotPaused);
        }

        stream.status = StreamStatus::Active;
        stream.last_update = env.ledger().timestamp();
        env.storage().persistent().set(&key, &stream);
        extend_instance_ttl(&env);
        extend_stream_ttl(&env, stream_id);
        ResumedStreamEvent { stream_id }.publish(&env);
        Ok(())
    }

    pub fn withdraw(env: Env, stream_id: u64) -> Result<i128, Error> {
        require_not_paused(&env)?;

        let key = DataKey::Stream(stream_id);
        let mut stream: Stream = env
            .storage()
            .persistent()
            .get(&key)
            .ok_or(Error::StreamNotFound)?;

        stream.ngo.require_auth();

        if stream.status == StreamStatus::Paused {
            return Err(Error::StreamPaused);
        }

        let now = env.ledger().timestamp();
        let elapsed = now.saturating_sub(accrual_base(&stream));
        let accrued = math::accrued(stream.rate, elapsed, stream.balance);

        if accrued <= 0 {
            return Err(Error::NothingToWithdraw);
        }

        record_payout(&mut stream, accrued)?;
        stream.last_update = now;
        let drained = stream.balance == 0;
        if drained {
            stream.status = StreamStatus::Drained;
        }
        env.storage().persistent().set(&key, &stream);
        extend_instance_ttl(&env);
        extend_stream_ttl(&env, stream_id);

        add_committed(&env, &stream.token, -accrued)?;

        let token_client = token::Client::new(&env, &stream.token);
        let payout = payout_destination(&env, &stream.ngo);
        let net = pay_ngo(&env, &token_client, &payout, accrued);
        let fee = fee_from_payout(accrued, net)?;

        WithdrawnEvent { stream_id, accrued, fee, net, payout }.publish(&env);

        if drained {
            DrainedEvent { stream_id, total_paid: stream.withdrawn }.publish(&env);
        }

        Ok(net)
    }

    pub fn withdraw_batch(env: Env, stream_ids: Vec<u64>) -> Result<Vec<i128>, Error> {
        require_not_paused(&env)?;

        if stream_ids.len() > MAX_BATCH_WITHDRAW_STREAMS {
            return Err(Error::BatchLimitExceeded);
        }

        let mut seen_ids: Map<u64, bool> = Map::new(&env);
        for stream_id in stream_ids.iter() {
            if seen_ids.contains_key(stream_id) {
                return Err(Error::DuplicateStreamId);
            }
            seen_ids.set(stream_id, true);
        }

        let now = env.ledger().timestamp();
        let mut ngo: Option<Address> = None;
        let mut net_amounts: Vec<i128> = Vec::new(&env);
        let mut accrued_amounts: Vec<i128> = Vec::new(&env);
        let mut net_payouts: Map<Address, i128> = Map::new(&env);
        let mut fee_payouts: Map<Address, i128> = Map::new(&env);
        let mut gross_payouts: Map<Address, i128> = Map::new(&env);
        let mut drained_totals: Map<u64, i128> = Map::new(&env);
        let mut total_accrued: i128 = 0;

        for stream_id in stream_ids.iter() {
            let key = DataKey::Stream(stream_id);
            let mut stream: Stream = env
                .storage()
                .persistent()
                .get(&key)
                .ok_or(Error::StreamNotFound)?;

            if stream.status == StreamStatus::Paused {
                return Err(Error::StreamPaused);
            }

            match &ngo {
                None => {
                    stream.ngo.require_auth();
                    ngo = Some(stream.ngo.clone());
                }
                Some(existing) if existing != &stream.ngo => return Err(Error::MixedNgo),
                Some(_) => {}
            }

            let elapsed = now.saturating_sub(accrual_base(&stream));
            let accrued = math::accrued(stream.rate, elapsed, stream.balance);
            accrued_amounts.push_back(accrued);
            total_accrued = total_accrued.checked_add(accrued).ok_or(Error::ArithmeticOverflow)?;

            let fee = if accrued > 0 { compute_fee(&env, &stream.token, accrued) } else { 0 };
            let net = accrued.checked_sub(fee).ok_or(Error::ArithmeticOverflow)?;
            net_amounts.push_back(net);

            if accrued <= 0 {
                continue;
            }

            record_payout(&mut stream, accrued)?;
            stream.last_update = now;
            if stream.balance == 0 {
                stream.status = StreamStatus::Drained;
                drained_totals.set(stream_id, stream.withdrawn);
            }
            env.storage().persistent().set(&key, &stream);
            extend_stream_ttl(&env, stream_id);

            let net_total = net_payouts.get(stream.token.clone()).unwrap_or(0);
            net_payouts.set(stream.token.clone(), net_total.checked_add(net).ok_or(Error::ArithmeticOverflow)?);

            if fee > 0 {
                let fee_total = fee_payouts.get(stream.token.clone()).unwrap_or(0);
                fee_payouts.set(stream.token.clone(), fee_total.checked_add(fee).ok_or(Error::ArithmeticOverflow)?);
            }

            let gross_total = gross_payouts.get(stream.token.clone()).unwrap_or(0);
            gross_payouts.set(stream.token.clone(), gross_total.checked_add(accrued).ok_or(Error::ArithmeticOverflow)?);
        }

        if total_accrued <= 0 {
            return Err(Error::NothingToWithdraw);
        }

        let mut total_fees: i128 = 0;
        for (_, fee) in fee_payouts.iter() {
            total_fees = total_fees.checked_add(fee).ok_or(Error::ArithmeticOverflow)?;
        }
        record_outflow_totals(&env, total_accrued, total_fees)?;

        for (token, gross) in gross_payouts.iter() {
            if gross > 0 {
                add_committed(&env, &token, -gross)?;
            }
        }

        let ngo = ngo.ok_or(Error::NothingToWithdraw)?;
        extend_instance_ttl(&env);
        let payout = payout_destination(&env, &ngo);

        for (token, net_total) in net_payouts.iter() {
            if net_total > 0 {
                let token_client = token::Client::new(&env, &token);
                token_client.transfer(&env.current_contract_address(), &payout, &net_total);
            }
        }

        let treasury: Option<Address> = env.storage().instance().get(&DataKey::Treasury);
        if let Some(treasury_address) = treasury {
            for (token, fee_total) in fee_payouts.iter() {
                if fee_total > 0 {
                    let token_client = token::Client::new(&env, &token);
                    token_client.transfer(&env.current_contract_address(), &treasury_address, &fee_total);
                }
            }
        }

        for (idx, stream_id) in stream_ids.iter().enumerate() {
            let idx = idx as u32;
            let accrued = accrued_amounts.get(idx).unwrap_or(0);
            let net = net_amounts.get(idx).unwrap_or(0);

            if accrued > 0 {
                let fee = fee_from_payout(accrued, net)?;
                WithdrawnEvent { stream_id, accrued, fee, net, payout: payout.clone() }.publish(&env);

                if let Some(total_paid) = drained_totals.get(stream_id) {
                    DrainedEvent { stream_id, total_paid }.publish(&env);
                }
            }
        }

        Ok(net_amounts)
    }

    pub fn withdraw_all(env: Env, ngo: Address, start: u32, limit: u32) -> Result<WithdrawAllPage, Error> {
        require_not_paused(&env)?;

        let index: Vec<u64> = env
            .storage()
            .persistent()
            .get(&DataKey::NgoStreams(ngo))
            .unwrap_or_else(|| Vec::new(&env));
        let total = index.len();

        if limit == 0 || start >= total {
            return Ok(WithdrawAllPage { amounts: Vec::new(&env), next: total });
        }

        let effective_limit = limit.min(MAX_BATCH_WITHDRAW_STREAMS);
        let end = start.checked_add(effective_limit).unwrap_or(total).min(total);

        let mut page = Vec::new(&env);
        for position in start..end {
            if let Some(stream_id) = index.get(position) {
                page.push_back(stream_id);
            }
        }

        let page_len = page.len();
        let amounts = match Self::withdraw_batch(env.clone(), page) {
            Ok(amounts) => amounts,
            Err(Error::NothingToWithdraw) => {
                let mut zeros = Vec::new(&env);
                for _ in 0..page_len {
                    zeros.push_back(0i128);
                }
                zeros
            }
            Err(error) => return Err(error),
        };

        Ok(WithdrawAllPage { amounts, next: end })
    }

    pub fn cancel_stream(env: Env, stream_id: u64) -> Result<i128, Error> {
        let key = DataKey::Stream(stream_id);
        let mut stream: Stream = env
            .storage()
            .persistent()
            .get(&key)
            .ok_or(Error::StreamNotFound)?;

        stream.donor.require_auth();

        if stream.cancelled {
            return Err(Error::StreamCancelled);
        }

        let now = env.ledger().timestamp();
        let accrued = settle_core(&env, &mut stream, now)?;
        let refund = stream.balance;

        stream.balance = 0;
        stream.rate = 0;
        stream.cancelled = true;
        stream.last_update = now;
        stream.status = StreamStatus::Cancelled;
        env.storage().persistent().set(&key, &stream);
        extend_instance_ttl(&env);
        let grace_ledgers = env.storage().instance().get(&DataKey::CancelGraceLedgers).unwrap_or(0);
        extend_cancelled_stream_ttl(&env, stream_id, grace_ledgers)?;

        let token_client = token::Client::new(&env, &stream.token);
        if refund > 0 {
            add_committed(&env, &stream.token, -refund)?;
            token_client.transfer(&env.current_contract_address(), &stream.donor, &refund);
        }

        if accrued > 0 || refund > 0 {
            CancelledStreamEvent { stream_id, accrued, refund }.publish(&env);
        }

        Ok(refund)
    }

    pub fn rescue_stream(env: Env, stream_id: u64) -> Result<i128, Error> {
        require_admin(&env)?;

        let paused: bool = env.storage().instance().get(&DataKey::Paused).unwrap_or(false);
        if !paused {
            return Err(Error::NotPaused);
        }

        let key = DataKey::Stream(stream_id);
        let mut stream: Stream = env
            .storage()
            .persistent()
            .get(&key)
            .ok_or(Error::StreamNotFound)?;

        if stream.cancelled {
            return Err(Error::StreamCancelled);
        }

        let refund = stream.balance;
        if refund > 0 {
            add_committed(&env, &stream.token, -refund)?;
            let token_client = token::Client::new(&env, &stream.token);
            token_client.transfer(&env.current_contract_address(), &stream.donor, &refund);
        }

        let now = env.ledger().timestamp();
        stream.balance = 0;
        stream.rate = 0;
        stream.cancelled = true;
        stream.last_update = now;
        stream.status = StreamStatus::Cancelled;
        env.storage().persistent().set(&key, &stream);
        extend_instance_ttl(&env);
        let grace_ledgers = env.storage().instance().get(&DataKey::CancelGraceLedgers).unwrap_or(0);
        extend_cancelled_stream_ttl(&env, stream_id, grace_ledgers)?;

        RescuedStreamEvent { stream_id, refund }.publish(&env);
        Ok(refund)
    }

    pub fn top_up(env: Env, stream_id: u64, amount: i128) -> Result<(), Error> {
        require_not_paused(&env)?;

        if amount <= 0 {
            return Err(Error::InvalidAmount);
        }
        if amount < Self::min_deposit(env.clone()) {
            return Err(Error::DepositTooLow);
        }

        let key = DataKey::Stream(stream_id);
        let mut stream: Stream = env
            .storage()
            .persistent()
            .get(&key)
            .ok_or(Error::StreamNotFound)?;

        stream.donor.require_auth();

        if stream.cancelled {
            return Err(Error::StreamCancelled);
        }

        require_verified_ngo(&env, &stream.ngo)?;

        let token_client = token::Client::new(&env, &stream.token);
        let now = env.ledger().timestamp();
        let settled = settle_core(&env, &mut stream, now)?;

        stream.balance = stream.balance.checked_add(amount).ok_or(Error::ArithmeticOverflow)?;

        if stream.status == StreamStatus::Drained && stream.balance > 0 {
            stream.status = StreamStatus::Active;
        }

        let total_donated: i128 = env.storage().instance().get(&DataKey::TotalDonated).unwrap_or(0);
        let next_total = total_donated.checked_add(amount).ok_or(Error::ArithmeticOverflow)?;

        env.storage().instance().set(&DataKey::TotalDonated, &next_total);
        env.storage().persistent().set(&key, &stream);
        extend_instance_ttl(&env);
        extend_stream_ttl(&env, stream_id);

        add_committed(&env, &stream.token, amount)?;
        token_client.transfer(&stream.donor, env.current_contract_address(), &amount);

        ToppedUpEvent { stream_id, amount, settled }.publish(&env);
        Ok(())
    }

    pub fn modify_rate(env: Env, stream_id: u64, new_rate: i128) -> Result<(), Error> {
        require_not_paused(&env)?;

        if new_rate <= 0 {
            return Err(Error::InvalidAmount);
        }
        if let Some(max_rate) = max_rate_cap(&env) {
            if new_rate > max_rate {
                return Err(Error::RateTooHigh);
            }
        }

        let key = DataKey::Stream(stream_id);
        let mut stream: Stream = env
            .storage()
            .persistent()
            .get(&key)
            .ok_or(Error::StreamNotFound)?;

        stream.donor.require_auth();
        let old_rate = stream.rate;

        if stream.cancelled {
            return Err(Error::StreamCancelled);
        }
        if new_rate == stream.rate {
            return Ok(());
        }
        if new_rate > stream.rate {
            require_verified_ngo(&env, &stream.ngo)?;
        }
        if depletes_before_min_duration(stream.balance, new_rate, min_duration_seconds(&env)) {
            return Err(Error::StreamTooShort);
        }

        let now = env.ledger().timestamp();
        let settled = settle(&env, stream_id, &mut stream, now)?;
        stream.rate = new_rate;

        env.storage().persistent().set(&key, &stream);
        extend_instance_ttl(&env);
        extend_stream_ttl(&env, stream_id);

        RateModifiedEvent { stream_id, old_rate, new_rate, settled }.publish(&env);
        Ok(())
    }

    pub fn transfer_stream(env: Env, stream_id: u64, new_ngo: Address) -> Result<(), Error> {
        require_not_paused(&env)?;

        let key = DataKey::Stream(stream_id);
        let mut stream: Stream = env
            .storage()
            .persistent()
            .get(&key)
            .ok_or(Error::StreamNotFound)?;

        stream.donor.require_auth();

        if stream.cancelled {
            return Err(Error::StreamCancelled);
        }
        if new_ngo == stream.donor {
            return Err(Error::SelfStream);
        }
        if new_ngo == stream.ngo {
            return Ok(());
        }

        require_verified_ngo(&env, &new_ngo)?;

        let old_ngo = stream.ngo.clone();
        let now = env.ledger().timestamp();
        let settled = settle(&env, stream_id, &mut stream, now)?;
        if stream.balance == 0 {
            stream.status = StreamStatus::Drained;
        }
        stream.ngo = new_ngo.clone();

        env.storage().persistent().set(&key, &stream);

        let old_key = DataKey::NgoStreams(old_ngo.clone());
        if let Some(old_streams) = env.storage().persistent().get::<_, Vec<u64>>(&old_key) {
            let mut remaining = Vec::new(&env);
            for id in old_streams.iter() {
                if id != stream_id {
                    remaining.push_back(id);
                }
            }
            env.storage().persistent().set(&old_key, &remaining);
        }
        let new_key = DataKey::NgoStreams(new_ngo.clone());
        let mut new_streams: Vec<u64> = env
            .storage()
            .persistent()
            .get(&new_key)
            .unwrap_or_else(|| Vec::new(&env));
        new_streams.push_back(stream_id);
        env.storage().persistent().set(&new_key, &new_streams);
        env.storage().persistent().extend_ttl(&new_key, STREAM_LIFETIME_THRESHOLD, STREAM_BUMP_AMOUNT);
        extend_instance_ttl(&env);
        extend_stream_ttl(&env, stream_id);

        TransferStreamEvent { stream_id, old_ngo, new_ngo, settled }.publish(&env);
        Ok(())
    }

    pub fn upgrade_delay(env: Env) -> u64 {
        get_upgrade_delay(&env)
    }

    pub fn pending_upgrade(env: Env) -> Option<PendingUpgrade> {
        env.storage().instance().get(&DataKey::PendingUpgrade)
    }

    pub fn committed_balance(env: Env, token: Address) -> i128 {
        get_committed(&env, &token)
    }

    pub fn surplus(env: Env, token: Address) -> i128 {
        let held = token::Client::new(&env, &token).balance(&env.current_contract_address());
        held.saturating_sub(get_committed(&env, &token))
    }

    pub fn set_upgrade_delay(env: Env, delay: u64) -> Result<(), Error> {
        require_admin(&env)?;
        env.storage().instance().set(&DataKey::UpgradeDelay, &delay);
        extend_instance_ttl(&env);
        UpgradeDelaySetEvent { delay }.publish(&env);
        Ok(())
    }

    pub fn propose_upgrade(env: Env, new_wasm_hash: BytesN<32>) -> Result<(), Error> {
        require_admin(&env)?;
        let delay = get_upgrade_delay(&env);
        let executable_at = env
            .ledger()
            .timestamp()
            .checked_add(delay)
            .ok_or(Error::ArithmeticOverflow)?;
        env.storage().instance().set(
            &DataKey::PendingUpgrade,
            &PendingUpgrade { new_wasm_hash: new_wasm_hash.clone(), executable_at },
        );
        extend_instance_ttl(&env);
        UpgradeProposedEvent { new_wasm_hash, executable_at }.publish(&env);
        Ok(())
    }

    pub fn execute_upgrade(env: Env) -> Result<(), Error> {
        require_admin(&env)?;
        let pending: PendingUpgrade = env
            .storage()
            .instance()
            .get(&DataKey::PendingUpgrade)
            .ok_or(Error::NoPendingUpgrade)?;
        if env.ledger().timestamp() < pending.executable_at {
            return Err(Error::UpgradeDelayNotElapsed);
        }
        env.storage().instance().remove(&DataKey::PendingUpgrade);
        env.deployer().update_current_contract_wasm(pending.new_wasm_hash.clone());
        extend_instance_ttl(&env);
        UpgradedEvent { new_wasm_hash: pending.new_wasm_hash }.publish(&env);
        Ok(())
    }

    pub fn cancel_upgrade(env: Env) -> Result<(), Error> {
        require_admin(&env)?;
        let pending: PendingUpgrade = env
            .storage()
            .instance()
            .get(&DataKey::PendingUpgrade)
            .ok_or(Error::NoPendingUpgrade)?;
        env.storage().instance().remove(&DataKey::PendingUpgrade);
        extend_instance_ttl(&env);
        UpgradeCancelledEvent { new_wasm_hash: pending.new_wasm_hash }.publish(&env);
        Ok(())
    }

    pub fn sweep_surplus(env: Env, token: Address, amount: i128) -> Result<(), Error> {
        require_admin(&env)?;
        require_not_paused(&env)?;

        if amount <= 0 {
            return Err(Error::InvalidAmount);
        }

        let treasury: Address = env
            .storage()
            .instance()
            .get(&DataKey::Treasury)
            .ok_or(Error::NoTreasury)?;

        let token_client = token::Client::new(&env, &token);
        let held = token_client.balance(&env.current_contract_address());
        let surplus = held.saturating_sub(get_committed(&env, &token));
        if amount > surplus {
            return Err(Error::SweepExceedsSurplus);
        }

        token_client.transfer(&env.current_contract_address(), &treasury, &amount);
        extend_instance_ttl(&env);
        SurplusSweptEvent { token, amount, treasury }.publish(&env);
        Ok(())
    }

    pub fn upgrade(env: Env, new_wasm_hash: BytesN<32>) -> Result<(), Error> {
        require_admin(&env)?;
        if get_upgrade_delay(&env) != 0 {
            return Err(Error::UpgradeTimelockEnabled);
        }
        env.deployer().update_current_contract_wasm(new_wasm_hash.clone());
        UpgradedEvent { new_wasm_hash }.publish(&env);
        Ok(())
    }
}

#[cfg(test)]
mod test;
