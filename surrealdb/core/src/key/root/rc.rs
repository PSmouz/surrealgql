//! Background reclaim queue
//!
//! `REMOVE NAMESPACE` / `REMOVE DATABASE` / `REMOVE INDEX` delete only the
//! catalog definition inside the user transaction (so the object becomes
//! immediately invisible) and enqueue a reclaim job here. A background task
//! (see [`crate::kvs::Datastore::reclaim_tombstones`]) periodically drains the
//! queue and destroys the now-orphaned data prefix out-of-band — via
//! `unsafe_destroy_range` on TiKV or a transactional prefix delete on other
//! backends.
//!
//! Because the queue entry is written in the same transaction that removes the
//! catalog definition, a cancelled/rolled-back transaction leaves neither the
//! removal nor the reclaim job behind, preserving ACID. The data is only ever
//! destroyed after the removal has committed.
//!
//! The reclaim job lives at the **root** level (`/!rc...`) precisely so it
//! survives the deletion of the namespace/database prefix it refers to.
use std::borrow::Cow;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use storekey::{BorrowDecode, Encode};
use uuid::Uuid;

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::{impl_kv_key_storekey, impl_kv_value_revisioned};
use crate::val::TableName;

/// Mutable state stored as the value of a [`ReclaimKey`].
///
/// `observed_ms` is the wall-clock unix-millis at which the background reclaim
/// task first *observed* this entry. The reclaim task only ever reads committed
/// entries, so this is necessarily at or after the removal's commit — unlike the
/// key's `uid`, which is a UUIDv7 stamped while the `REMOVE` statement runs
/// (before commit, and arbitrarily early inside a long `BEGIN`/`COMMIT` block).
/// The snapshot-safety grace is therefore measured from `observed_ms`, never
/// from the pre-commit `uid`. `0` means "not yet observed".
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ReclaimState {
	pub observed_ms: u64,
}

impl_kv_value_revisioned!(ReclaimState);

/// Reclaim job targeting an entire namespace prefix (`/*{ns}`).
pub(crate) const RECLAIM_NAMESPACE: u8 = 0;
/// Reclaim job targeting an entire database prefix (`/*{ns}*{db}`).
pub(crate) const RECLAIM_DATABASE: u8 = 1;
/// Reclaim job targeting a single index prefix (`/*{ns}*{db}*{tb}+{ix}`).
pub(crate) const RECLAIM_INDEX: u8 = 2;

/// Represents an entry in the background reclaim queue.
///
/// The `kind` discriminant selects which prefix the reclaim task destroys; the
/// `ns`/`db`/`tb`/`ix` ids identify it. Fields not relevant to a given `kind`
/// are zero/empty. `expunge` records whether the data must be hard-cleared
/// (all MVCC versions) rather than soft-deleted. `uid` is a unique,
/// time-ordered id that disambiguates entries.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct ReclaimKey<'key> {
	__: u8,
	_a: u8,
	_b: u8,
	_c: u8,
	pub kind: u8,
	pub ns: NamespaceId,
	pub db: DatabaseId,
	pub tb: Cow<'key, TableName>,
	pub ix: IndexId,
	pub expunge: u8,
	pub uid: Uuid,
}

impl_kv_key_storekey!(ReclaimKey<'_> => ReclaimState);

impl Categorise for ReclaimKey<'_> {
	fn categorise(&self) -> Category {
		Category::Reclaim
	}
}

impl<'key> ReclaimKey<'key> {
	/// Enqueue reclaim of a whole namespace prefix.
	pub(crate) fn namespace(ns: NamespaceId, expunge: bool, uid: Uuid) -> Self {
		Self::new(
			RECLAIM_NAMESPACE,
			ns,
			DatabaseId(0),
			Cow::Owned(TableName::from("")),
			IndexId(0),
			expunge,
			uid,
		)
	}

	/// Enqueue reclaim of a whole database prefix.
	pub(crate) fn database(ns: NamespaceId, db: DatabaseId, expunge: bool, uid: Uuid) -> Self {
		Self::new(
			RECLAIM_DATABASE,
			ns,
			db,
			Cow::Owned(TableName::from("")),
			IndexId(0),
			expunge,
			uid,
		)
	}

	/// Enqueue reclaim of a single index prefix.
	pub(crate) fn index(
		ns: NamespaceId,
		db: DatabaseId,
		tb: Cow<'key, TableName>,
		ix: IndexId,
		expunge: bool,
		uid: Uuid,
	) -> Self {
		Self::new(RECLAIM_INDEX, ns, db, tb, ix, expunge, uid)
	}

	fn new(
		kind: u8,
		ns: NamespaceId,
		db: DatabaseId,
		tb: Cow<'key, TableName>,
		ix: IndexId,
		expunge: bool,
		uid: Uuid,
	) -> Self {
		Self {
			__: b'/',
			_a: b'!',
			_b: b'r',
			_c: b'c',
			kind,
			ns,
			db,
			tb,
			ix,
			expunge: expunge as u8,
			uid,
		}
	}

	/// Half-open byte range covering every reclaim queue entry.
	pub(crate) fn range() -> (Vec<u8>, Vec<u8>) {
		(b"/!rc\x00".to_vec(), b"/!rc\xff".to_vec())
	}

	pub(crate) fn decode_key(k: &[u8]) -> anyhow::Result<ReclaimKey<'_>> {
		Ok(storekey::decode_borrow(k)?)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::KVKey;

	#[test]
	fn range() {
		assert_eq!(ReclaimKey::range(), (b"/!rc\x00".to_vec(), b"/!rc\xff".to_vec()));
	}

	#[test]
	fn database_key_roundtrips() {
		let val = ReclaimKey::database(NamespaceId(1), DatabaseId(2), false, Uuid::from_u128(7));
		let enc = ReclaimKey::encode_key(&val).unwrap();
		// Inside the scannable range
		let (beg, end) = ReclaimKey::range();
		assert!(enc.as_slice() >= beg.as_slice() && enc.as_slice() < end.as_slice());
		let dec = ReclaimKey::decode_key(&enc).unwrap();
		assert_eq!(dec.kind, RECLAIM_DATABASE);
		assert_eq!(dec.ns, NamespaceId(1));
		assert_eq!(dec.db, DatabaseId(2));
		assert_eq!(dec.expunge, 0);
		assert_eq!(dec.uid, Uuid::from_u128(7));
	}

	#[test]
	fn index_key_roundtrips() {
		let val = ReclaimKey::index(
			NamespaceId(4),
			DatabaseId(5),
			Cow::Owned(TableName::from("testtb")),
			IndexId(6),
			true,
			Uuid::from_u128(9),
		);
		let enc = ReclaimKey::encode_key(&val).unwrap();
		let dec = ReclaimKey::decode_key(&enc).unwrap();
		assert_eq!(dec.kind, RECLAIM_INDEX);
		assert_eq!(dec.ns, NamespaceId(4));
		assert_eq!(dec.db, DatabaseId(5));
		assert_eq!(dec.tb.as_ref(), &TableName::from("testtb"));
		assert_eq!(dec.ix, IndexId(6));
		assert_eq!(dec.expunge, 1);
	}
}
