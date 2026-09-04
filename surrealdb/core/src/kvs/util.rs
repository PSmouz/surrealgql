use std::ops::Range;
use std::sync::Arc;

use anyhow::Result;

use super::direction::Direction;
use crate::kvs::{KVKey, KVValue, Key};

/// Advances a key to the next value,
/// can be used to skip over a certain key.
pub fn advance_key(key: &mut [u8]) {
	for b in key.iter_mut().rev() {
		*b = b.wrapping_add(1);
		if *b != 0 {
			break;
		}
	}
}

/// Advance a resume-by-bound cursor's range so the next chunk resumes strictly
/// after `last` (the last key visited this chunk). Mirrors the successor logic
/// in `DefaultValsCursor`/`DefaultKeysCursor`: forward appends `0x00` to get the
/// minimal key greater than `last` (the half-open `start` already excludes it
/// otherwise); backward clips `end` to `last` (the half-open `end` already
/// excludes it). A `None` `last` means nothing was visited — leave the range.
pub(crate) fn update_range(rng: &mut Range<Key>, dir: Direction, last: Option<&[u8]>) {
	let Some(last) = last else {
		return;
	};
	match dir {
		Direction::Forward => {
			rng.start.clear();
			rng.start.extend_from_slice(last);
			rng.start.push(0x00);
		}
		Direction::Backward => {
			rng.end.clear();
			rng.end.extend_from_slice(last);
		}
	}
}

pub fn to_prefix_range<K: KVKey>(key: &K) -> Result<Range<Vec<u8>>> {
	let start = key.encode_key()?;
	let mut end = start.clone();
	end.push(0xff);
	Ok(Range {
		start,
		end,
	})
}

/// Takes an iterator of byte slices and deserializes the byte slices to the
/// expected type, returning an error if any of the values fail to serialize.
///
/// Bound to `KeyContext = ()` to prevent accidental use on `Record`
/// (whose decode requires a `RecordId` from the storage key).
pub fn deserialize_cache<'a, I, T>(iter: I) -> Result<Arc<[T]>>
where
	T: KVValue<KeyContext = ()>,
	I: Iterator<Item = &'a [u8]>,
{
	let mut buf = Vec::new();
	for slice in iter {
		buf.push(T::kv_decode_value(slice, ())?)
	}
	Ok(Arc::from(buf))
}
