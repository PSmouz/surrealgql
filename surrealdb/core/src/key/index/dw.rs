//! Shard-prefixed pending updates for DiskANN indexes.
//!
//! This is the sharded successor to the legacy [`crate::key::index::dr`] (`!dr`) layout. The
//! record key is prefixed with the writer's pending-state shard (the same shard the writer bumps
//! in `!dp`), so compaction can drain — and lookup can scan — one shard at a time instead of
//! sweeping the whole index's pending range on every query while a write backlog exists.
//!
//! Legacy `!dr` keys written by an older binary are migrated lazily: writers only ever emit `!dw`
//! keys, while compaction and lookup keep reading the legacy `!dr` range until it drains empty
//! (the dual-read transition). The two layouts use distinct key tags so their ranges never
//! overlap and a scan can decode each unambiguously.

use std::borrow::Cow;
use std::ops::Range;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::trees::diskann::DiskAnnRecordPendingUpdate;
use crate::kvs::{KVKey, Key, impl_kv_key_storekey};
use crate::val::{IndexFormat, RecordIdKey, TableName};

/// Stores the coalesced pending update for one DiskANN indexed record, prefixed by its shard.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "IndexFormat")]
pub(crate) struct DiskAnnRecordPendingShard<'a> {
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
	pub id: Cow<'a, RecordIdKey>,
}

impl KVKey for DiskAnnRecordPendingShard<'_> {
	type ValueType = DiskAnnRecordPendingUpdate;

	fn encode_key(&self) -> Result<Key> {
		Ok(storekey::encode_vec_format::<IndexFormat, _>(self)
			.map_err(|_| crate::err::Error::Unencodable)?)
	}

	fn value_context(&self) {}
}

impl<'a> DiskAnnRecordPendingShard<'a> {
	/// Creates the `!dw{shard}{record_key}` pending-operation key.
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		shard: u16,
		id: &'a RecordIdKey,
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
			_g: b'w',
			shard,
			id: Cow::Borrowed(id),
		}
	}

	/// Decodes a `!dw` key scanned during lookup or compaction.
	pub(crate) fn decode_key(k: &[u8]) -> Result<DiskAnnRecordPendingShard<'_>> {
		Ok(storekey::decode_borrow_format::<IndexFormat, _>(k)?)
	}
}

/// Prefix used to build the range covering all `!dw` pending updates for one shard of one index.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode)]
#[storekey(format = "()")]
pub(crate) struct DiskAnnRecordPendingShardPrefix<'a> {
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

impl_kv_key_storekey!(DiskAnnRecordPendingShardPrefix<'_> => ());

impl<'a> DiskAnnRecordPendingShardPrefix<'a> {
	/// Returns the range covering record-keyed DiskANN pending updates for one shard.
	pub(crate) fn range(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		shard: u16,
	) -> Result<Range<Key>> {
		let mut beg = Self {
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
			_g: b'w',
			shard,
		}
		.encode_key()?;
		let mut end = beg.clone();
		beg.push(0);
		end.push(0xff);
		Ok(beg..end)
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::*;
	use crate::key::index::dr::DiskAnnRecordPendingPrefix;

	#[test]
	fn key() {
		let tb = TableName::from("testtb");
		let id = RecordIdKey::String(Strand::new_static("testid"));
		let val =
			DiskAnnRecordPendingShard::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), 7, &id);
		let enc = DiskAnnRecordPendingShard::encode_key(&val).unwrap();
		let dec = DiskAnnRecordPendingShard::decode_key(&enc).unwrap();
		assert_eq!(dec.shard, 7);
		assert_eq!(dec.id.as_ref(), &id);
	}

	#[test]
	fn shard_range_is_disjoint_per_shard_and_from_legacy() {
		let tb = TableName::from("testtb");
		// One shard's range must not contain a key from an adjacent shard.
		let id = RecordIdKey::Number(42);
		let key_shard_7 =
			DiskAnnRecordPendingShard::new(NamespaceId(1), DatabaseId(2), &tb, IndexId(3), 7, &id)
				.encode_key()
				.unwrap();
		let range_7 = DiskAnnRecordPendingShardPrefix::range(
			NamespaceId(1),
			DatabaseId(2),
			&tb,
			IndexId(3),
			7,
		)
		.unwrap();
		let range_8 = DiskAnnRecordPendingShardPrefix::range(
			NamespaceId(1),
			DatabaseId(2),
			&tb,
			IndexId(3),
			8,
		)
		.unwrap();
		assert!(range_7.start <= key_shard_7 && key_shard_7 < range_7.end);
		assert!(!(range_8.start <= key_shard_7 && key_shard_7 < range_8.end));

		// The legacy `!dr` range and the sharded `!dw` range must be disjoint so a scan of one
		// never decodes a key from the other.
		let legacy_range =
			DiskAnnRecordPendingPrefix::range(NamespaceId(1), DatabaseId(2), &tb, IndexId(3))
				.unwrap();
		assert!(!(legacy_range.start <= key_shard_7 && key_shard_7 < legacy_range.end));
	}
}
