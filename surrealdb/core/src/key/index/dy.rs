//! DiskANN sharded pending-state guard key.
//!
//! This is the `!dw` (sharded) analogue of [`crate::key::index::dp`] (`!dp`). New writes track
//! their pending state here, keyed by the writer's shard, so it is decoupled from the legacy `!dp`
//! guard. A pre-change node's compactor only ever clears `!dp` (it has no knowledge of
//! `!dw`/`!dy`), so keeping the sharded guard in its own family means an old compactor can never
//! mark a shard empty while a `!dw` entry it cannot see still exists — which would otherwise hide
//! that record from upgraded lookups during a mixed-version rolling upgrade.

use std::borrow::Cow;

use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::trees::diskann::DiskAnnPendingState;
use crate::kvs::impl_kv_key_storekey;
use crate::val::TableName;

/// Stores one shard of the sharded (`!dw`) pending-operation summary for one DiskANN index.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Dy<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub tb: Cow<'a, TableName>,
	_d: u8,
	pub ix: IndexId,
	_e: u8,
	_f: u8,
	_g: u8,
	pub shard: u16,
}

impl_kv_key_storekey!(Dy<'_> => DiskAnnPendingState);

impl<'a> Dy<'a> {
	/// Creates one `!dy` sharded pending-state guard shard key for one DiskANN index.
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		shard: u16,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb: Cow::Borrowed(tb),
			_d: b'+',
			ix,
			_e: b'!',
			_f: b'd',
			_g: b'y',
			shard,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::key::index::dp::Dp;
	use crate::kvs::KVKey;

	#[test]
	fn sharded_guard_key_is_distinct_from_legacy_guard() {
		let tb = TableName::from("testtb");
		let dy = Dy::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), 7).encode_key().unwrap();
		let dp = Dp::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), 7).encode_key().unwrap();
		// The sharded guard must not collide with the legacy guard an old compactor clears.
		assert_ne!(dy, dp);
	}
}
