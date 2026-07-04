use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use mooncache_common::{CacheKey, TenantId};
use tokio::sync::watch;

use crate::routes::GatewayResponse;

/// Per-key waiter cap sized for the in-process hot-key load smoke while still bounding retained receivers.
pub(crate) const DEFAULT_MAX_WAITERS_PER_KEY: usize = 1024;

type SharedResult = Result<GatewayResponse, String>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SingleflightKey {
    tenant_id: TenantId,
    cache_key: CacheKey,
    write_mode: SingleflightWriteMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SingleflightWriteMode {
    Writable,
    ReadOnly,
}

impl SingleflightKey {
    pub(crate) fn new(
        tenant_id: TenantId,
        cache_key: CacheKey,
        write_mode: SingleflightWriteMode,
    ) -> Self {
        Self {
            tenant_id,
            cache_key,
            write_mode,
        }
    }
}

pub(crate) struct SingleflightGroup {
    inner: Arc<Mutex<HashMap<SingleflightKey, SharedInFlight>>>,
    max_waiters_per_key: usize,
}

impl SingleflightGroup {
    pub(crate) fn new(max_waiters_per_key: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            max_waiters_per_key,
        }
    }

    pub(crate) fn begin(
        &self,
        key: SingleflightKey,
    ) -> Result<SingleflightStart, SingleflightError> {
        let mut inner = self.inner()?;
        if let Some(in_flight) = inner.get_mut(&key) {
            if in_flight.waiters >= self.max_waiters_per_key {
                return Ok(SingleflightStart::OverCapacity);
            }
            in_flight.waiters += 1;
            return Ok(SingleflightStart::Waiter(SingleflightWaiter {
                receiver: in_flight.sender.subscribe(),
            }));
        }

        let (sender, _receiver) = watch::channel(None);
        inner.insert(key.clone(), SharedInFlight { sender, waiters: 0 });
        Ok(SingleflightStart::Leader(SingleflightLeader {
            key,
            inner: Arc::clone(&self.inner),
            completed: false,
        }))
    }

    pub(crate) fn publish(&self, mut leader: SingleflightLeader, result: SharedResult) {
        let Ok(mut inner) = self.inner() else {
            return;
        };
        let in_flight = inner.remove(&leader.key);
        leader.completed = true;
        drop(inner);

        if let Some(in_flight) = in_flight {
            let _ = in_flight.sender.send(Some(result));
        }
    }

    fn inner(
        &self,
    ) -> Result<MutexGuard<'_, HashMap<SingleflightKey, SharedInFlight>>, SingleflightError> {
        self.inner
            .lock()
            .map_err(|_| SingleflightError::PoisonedLock)
    }
}

impl Default for SingleflightGroup {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_WAITERS_PER_KEY)
    }
}

struct SharedInFlight {
    sender: watch::Sender<Option<SharedResult>>,
    waiters: usize,
}

pub(crate) enum SingleflightStart {
    Leader(SingleflightLeader),
    Waiter(SingleflightWaiter),
    OverCapacity,
}

pub(crate) struct SingleflightLeader {
    key: SingleflightKey,
    inner: Arc<Mutex<HashMap<SingleflightKey, SharedInFlight>>>,
    completed: bool,
}

impl Drop for SingleflightLeader {
    fn drop(&mut self) {
        if self.completed {
            return;
        }

        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        inner.remove(&self.key);
    }
}

pub(crate) struct SingleflightWaiter {
    receiver: watch::Receiver<Option<SharedResult>>,
}

impl SingleflightWaiter {
    pub(crate) async fn wait(mut self) -> SharedResult {
        loop {
            if let Some(result) = self.receiver.borrow().clone() {
                return result;
            }

            if self.receiver.changed().await.is_err() {
                return Err("singleflight leader dropped before publishing result".to_owned());
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum SingleflightError {
    PoisonedLock,
}
