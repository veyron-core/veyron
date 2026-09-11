use crate::utils::errors::VynkorError;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

pub use vynkor_wire::framing::{
    parse_frag_header, serialize_header, target_as_str, FragmentHeader, Frame, COMPRESS_THRESHOLD,
    FLAG_COMPRESSED, FLAG_FRAGMENTED, FLAG_MAC_PRESENT, FLAG_RAW_BINARY, FRAG_HEADER_SIZE,
    MAX_PAYLOAD_SIZE,
};

// frame magic shared by every kernel-side builder; layout owned by
// vynkor-wire (docs/FRAMING.md)
const FRAME_MAGIC: u16 = 0x5652;

/// Fixed-width routing key: target padded with NULs to the 32-byte header
/// field, truncated past 32 bytes.
pub fn target_bytes(target: &str) -> [u8; 32] {
    let mut t = [0u8; 32];
    let len = target.len().min(32);
    t[..len].copy_from_slice(&target.as_bytes()[..len]);
    t
}

/// Build an unsigned kernel-originated frame. Payload accepts anything
/// `Into<Arc<[u8]>>` (Vec<u8>, Arc<[u8]>) so callers keep their existing
/// allocation shape.
pub fn build_frame(target: &str, flags: u16, payload: impl Into<Arc<[u8]>>) -> Frame {
    let payload = payload.into();
    Frame {
        magic: FRAME_MAGIC,
        flags,
        length: payload.len() as u32,
        target: target_bytes(target),
        crc32: crc32fast::hash(&payload),
        payload,
        mac: None,
    }
}

pub async fn write_frame<W>(
    stream: &mut W,
    target: &str,
    flags: u16,
    payload: &[u8],
) -> Result<(), VynkorError>
where
    W: AsyncWrite + Unpin,
{
    vynkor_wire::framing::write_frame(stream, target, flags, payload)
        .await
        .map_err(Into::into)
}

pub async fn write_frame_raw<W>(stream: &mut W, frame: &Frame) -> Result<(), VynkorError>
where
    W: AsyncWrite + Unpin,
{
    vynkor_wire::framing::write_frame_raw(stream, frame)
        .await
        .map_err(Into::into)
}

pub async fn read_frame<R>(stream: &mut R) -> Result<Frame, VynkorError>
where
    R: AsyncRead + Unpin,
{
    vynkor_wire::framing::read_frame(stream)
        .await
        .map_err(Into::into)
}

pub async fn read_frame_with_timeout<R>(
    stream: &mut R,
    timeout: std::time::Duration,
) -> Result<Frame, VynkorError>
where
    R: AsyncRead + Unpin,
{
    vynkor_wire::framing::read_frame_with_timeout(stream, timeout)
        .await
        .map_err(Into::into)
}

/// Parse one full wire frame from an in-memory buffer — the WebSocket gateway
/// and `role: client` bridge deliver a complete binary message per frame, so a
/// whole frame's bytes are already at hand (no incremental stream read).
///
/// This is the kernel's single frame parser: it delegates to
/// `vynkor_wire::framing::read_frame`, so `FLAG_COMPRESSED` is decompressed
/// and normalized exactly as on the UDS read path (payload is plaintext, and
/// flags/length/crc32 describe the plaintext). `FLAG_FRAGMENTED` is rejected —
/// neither the gateway nor the bridge has a reassembly layer (WebSocket already
/// frames messages), so a fragment would reach the router half-parsed.
pub async fn parse_frame(data: &[u8]) -> Result<Frame, VynkorError> {
    let mut slice = data;
    let frame = vynkor_wire::framing::read_frame(&mut slice)
        .await
        .map_err(VynkorError::from)?;
    if frame.flags & FLAG_FRAGMENTED != 0 {
        return Err(VynkorError::Internal(
            "fragmented inbound frames not supported over WebSocket".to_string(),
        ));
    }
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_bytes_pads_to_32_and_truncates() {
        let t = target_bytes("self");
        assert_eq!(&t[..4], b"self");
        assert!(t[4..].iter().all(|&b| b == 0));
        let long = target_bytes(&"x".repeat(40));
        assert_eq!(long, [b'x'; 32]);
    }

    #[test]
    fn build_frame_sets_magic_length_crc_and_target() {
        let payload = vec![1u8, 2, 3];
        let frame = build_frame("echo", FLAG_COMPRESSED, payload);
        assert_eq!(frame.magic, FRAME_MAGIC);
        assert_eq!(frame.flags, FLAG_COMPRESSED);
        assert_eq!(frame.length, 3);
        assert_eq!(frame.crc32, crc32fast::hash(&[1u8, 2, 3]));
        assert_eq!(target_as_str(&frame), Some("echo"));
        assert!(frame.mac.is_none());
    }

    #[test]
    fn build_frame_accepts_vec_and_arc_payloads_without_type_change() {
        let from_vec = build_frame("a", 0, vec![7u8; 8]);
        let arc: Arc<[u8]> = vec![7u8; 8].into();
        let from_arc = build_frame("a", 0, arc);
        assert_eq!(from_vec.payload, from_arc.payload);
        assert_eq!(from_vec.crc32, from_arc.crc32);
    }
}
