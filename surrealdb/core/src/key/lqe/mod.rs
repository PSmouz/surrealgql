//! Stores live-query change events in a dedicated keyspace.
//!
//! This is intentionally separate from the user-facing changefeed
//! (`crate::key::change`, section marker `#`). It uses the same big-endian
//! versionstamp ordering, but the section marker `%` keeps it out of every
//! changefeed range scan and `SHOW CHANGES`, and lets live queries have their
//! own value format, `store_diff` policy, and retention. See [`crate::lq`].
use std::borrow::Cow;

use anyhow::Result;
use storekey::{BorrowDecode, Encode};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::key::category::{Categorise, Category};
use crate::kvs::impl_kv_key_storekey;
use crate::lq::event::LiveEvents;
use crate::val::TableName;

// Lqe stands for live-query event.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
#[storekey(format = "()")]
pub(crate) struct Lqe<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_d: u8,
	// ts is the commit versionstamp, encoded big-endian.
	pub ts: Cow<'a, [u8]>,
	_c: u8,
	pub tb: Cow<'a, TableName>,
}
impl_kv_key_storekey!(Lqe<'_> => LiveEvents);

impl Categorise for Lqe<'_> {
	fn categorise(&self) -> Category {
		Category::LiveQueryEvent
	}
}

impl<'a> Lqe<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, ts: &'a [u8], tb: &'a TableName) -> Self {
		Lqe {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_d: b'%',
			ts: Cow::Borrowed(ts),
			_c: b'*',
			tb: Cow::Borrowed(tb),
		}
	}

	/// Decode a live-query event key from its encoded bytes. The router uses
	/// this while tailing the keyspace to recover the table name and commit
	/// versionstamp of each scanned entry.
	pub fn decode_key(k: &[u8]) -> Result<Lqe<'_>> {
		Ok(storekey::decode_borrow(k)?)
	}
}

/// Create a complete live-query event key with timestamp.
pub fn new<'a>(ns: NamespaceId, db: DatabaseId, ts: &'a [u8], tb: &'a TableName) -> Lqe<'a> {
	Lqe::new(ns, db, ts, tb)
}

/// A prefix for the database's live-query events at/since a specific timestamp.
/// Used to build range scans (e.g. for garbage collection and, later, the
/// router's cursor reads).
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub struct LqeTsRange<'a> {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	pub ts: Cow<'a, [u8]>,
}

impl<'a> LqeTsRange<'a> {
	pub fn new(ns: NamespaceId, db: DatabaseId, ts: &'a [u8]) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'%',
			ts: Cow::Borrowed(ts),
		}
	}
}

impl_kv_key_storekey!(LqeTsRange<'_> => LiveEvents);

/// Returns the prefix for the database's live-query events at/since `ts`.
pub fn prefix_ts(ns: NamespaceId, db: DatabaseId, ts: &[u8]) -> LqeTsRange<'_> {
	LqeTsRange::new(ns, db, ts)
}

/// Upper bound for scanning a database's live-query event section. The router's
/// tail reader scans [`prefix_ts`]`(cursor)..`[`suffix`] to read every event
/// since its cursor; `0xff` sorts after any encoded `ts`/`tb` entry for the
/// database.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Encode, BorrowDecode)]
pub struct LqeRange {
	__: u8,
	_a: u8,
	pub ns: NamespaceId,
	_b: u8,
	pub db: DatabaseId,
	_c: u8,
	_xx: u8,
}

impl LqeRange {
	pub fn new_suffix(ns: NamespaceId, db: DatabaseId) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'%',
			_xx: 0xff,
		}
	}

	/// Lower bound of a database's section (`0x00` sorts before any `ts`). Only
	/// tests need an absolute lower bound; the router scans from [`prefix_ts`].
	#[cfg(test)]
	pub fn new_prefix(ns: NamespaceId, db: DatabaseId) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'%',
			_xx: 0x00,
		}
	}
}

impl_kv_key_storekey!(LqeRange => LiveEvents);

/// Returns the upper bound of a database's live-query event section.
pub fn suffix(ns: NamespaceId, db: DatabaseId) -> LqeRange {
	LqeRange::new_suffix(ns, db)
}

/// Returns the lower bound of a database's live-query event section (test-only:
/// the router scans from its cursor via [`prefix_ts`], not the absolute start).
#[cfg(test)]
pub fn prefix(ns: NamespaceId, db: DatabaseId) -> LqeRange {
	LqeRange::new_prefix(ns, db)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::{HlcTimeStampImpl, KVKey, TimeStampImpl};

	#[test]
	fn lqe_key_uses_percent_section_marker() {
		let ts_impl = HlcTimeStampImpl;
		let buf = &mut [0u8; _];
		let ts = ts_impl.create_from_versionstamp(12345).unwrap().encode(buf);
		let tb = TableName::from("test");
		let enc = Lqe::new(NamespaceId(1), DatabaseId(2), ts, &tb).encode_key().unwrap();
		// Byte layout: '/' '*' <ns:4> '*' <db:4> '%' ...
		assert_eq!(&enc[0..2], b"/*");
		assert_eq!(enc[11], b'%', "section marker must be '%', distinct from changefeed '#'");
	}

	#[test]
	fn lqe_range_is_disjoint_from_changefeed_range() {
		// The changefeed range for a db is bounded within the '#' (0x23) section;
		// the lqe '%' (0x25) section sorts strictly after it, so a changefeed
		// range scan can never include an lqe key and vice versa. Proven below on
		// real encoded keys rather than the literal markers.
		let ts_impl = HlcTimeStampImpl;
		let buf = &mut [0u8; _];
		let ts = ts_impl.create_from_versionstamp(1).unwrap().encode(buf);
		let cf_suffix =
			crate::key::change::suffix(NamespaceId(1), DatabaseId(2)).encode_key().unwrap();
		let lqe = LqeTsRange::new(NamespaceId(1), DatabaseId(2), ts).encode_key().unwrap();
		assert!(lqe > cf_suffix, "lqe keys must sort after the changefeed suffix");
	}
}
