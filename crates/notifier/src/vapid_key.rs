//! Parses `VAPID_PRIVATE_KEY` once, at startup, in whichever of the common
//! formats it arrived in, and checks it against `VAPID_PUBLIC_KEY`.
//!
//! Why several formats: `web_push::VapidSignatureBuilder::from_pem` only
//! takes a well-formed PEM, and failed on every send with
//! `MissingCryptoKeys` when the secret held something else -- most often
//! the raw base64url key `npx web-push generate-vapid-keys` prints, or a
//! PEM whose newlines were flattened into literal `\n` or spaces on the way
//! into a Secret. Rotating the key to fix the format would invalidate every
//! browser's push subscription (each is bound to the public key), so the
//! notifier accepts the key as it is instead.
//!
//! Accepted, tried in this order:
//! 1. `pem`: PEM, SEC1 (`EC PRIVATE KEY`) or PKCS#8 (`PRIVATE KEY`).
//! 2. `pem-escaped`: the same PEM with its newlines escaped as literal `\n`
//!    (or `\r\n`) or collapsed into spaces. Re-wrapped, then parsed.
//! 3. `base64url-raw`: the raw 32-byte P-256 private scalar, base64url or
//!    standard base64, with or without `=` padding.
//!
//! No error or log line here ever contains key material.

use std::fmt;
use std::fmt::Write as _;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use web_push::{PartialVapidSignatureBuilder, SubscriptionInfo, VapidSignatureBuilder};

/// What every "unrecognised key" error tells the operator.
pub(crate) const ACCEPTED_FORMATS: &str = "PEM (SEC1 \"EC PRIVATE KEY\" or PKCS#8 \
     \"PRIVATE KEY\"); that PEM with its newlines escaped as literal \\n or collapsed into \
     spaces; or the raw 32-byte P-256 private key as base64url or base64, padding optional \
     (what `npx web-push generate-vapid-keys` prints)";

/// The format `VAPID_PRIVATE_KEY` was found in. Logged at startup by name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VapidKeyFormat {
    Pem,
    PemEscaped,
    Base64UrlRaw,
}

impl VapidKeyFormat {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Pem => "pem",
            Self::PemEscaped => "pem-escaped",
            Self::Base64UrlRaw => "base64url-raw",
        }
    }
}

/// Why the VAPID key pair was refused. Messages never echo the input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VapidKeyError {
    /// `VAPID_PRIVATE_KEY` is in none of the accepted formats.
    UnrecognisedPrivateKey,
    /// `VAPID_PUBLIC_KEY` is not base64url/base64.
    PublicKeyNotBase64,
    /// The configured public key is not the one the private key derives.
    PublicKeyMismatch,
}

impl fmt::Display for VapidKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnrecognisedPrivateKey => write!(
                f,
                "VAPID_PRIVATE_KEY is not a valid P-256 private key in any accepted format. \
                 Accepted: {ACCEPTED_FORMATS}"
            ),
            Self::PublicKeyNotBase64 => f.write_str(
                "VAPID_PUBLIC_KEY is not base64url (expected the 65-byte uncompressed P-256 \
                 public key, base64url-encoded)",
            ),
            Self::PublicKeyMismatch => f.write_str(
                "VAPID_PUBLIC_KEY does not match the public key derived from \
                 VAPID_PRIVATE_KEY; refusing to start, since push services would reject \
                 every send to subscriptions created with VAPID_PUBLIC_KEY. Configure the \
                 matching pair (do not rotate unless you mean to invalidate all subscriptions)",
            ),
        }
    }
}

impl std::error::Error for VapidKeyError {}

/// The parsed private key, ready to sign for any subscription. `Clone` is
/// an `Arc` bump.
#[derive(Clone)]
pub(crate) struct VapidKey {
    builder: Arc<PartialVapidSignatureBuilder>,
    format: VapidKeyFormat,
}

impl fmt::Debug for VapidKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VapidKey")
            .field("key", &"<redacted>")
            .field("format", &self.format)
            .finish_non_exhaustive()
    }
}

impl VapidKey {
    /// Parses `input` in the first accepted format that fits.
    pub(crate) fn parse(input: &str) -> Result<Self, VapidKeyError> {
        let input = strip_matching_quotes(input.trim());
        let (builder, format) = if input.contains("-----BEGIN") {
            parse_pem(input)?
        } else {
            (parse_raw_base64(input)?, VapidKeyFormat::Base64UrlRaw)
        };
        Ok(Self {
            builder: Arc::new(builder),
            format,
        })
    }

    pub(crate) const fn format(&self) -> VapidKeyFormat {
        self.format
    }

    /// The uncompressed (65-byte) public key this private key derives.
    pub(crate) fn public_key(&self) -> Vec<u8> {
        self.builder.get_public_key()
    }

    /// Refuses a `VAPID_PUBLIC_KEY` that is not this key's own public key.
    pub(crate) fn verify_public_key(&self, configured: &str) -> Result<(), VapidKeyError> {
        let configured = decode_base64_lenient(strip_matching_quotes(configured.trim()))
            .ok_or(VapidKeyError::PublicKeyNotBase64)?;
        if configured == self.public_key() {
            Ok(())
        } else {
            Err(VapidKeyError::PublicKeyMismatch)
        }
    }

    /// A signature builder for one subscription.
    pub(crate) fn signature_builder<'a>(
        &self,
        subscription_info: &'a SubscriptionInfo,
    ) -> VapidSignatureBuilder<'a> {
        PartialVapidSignatureBuilder::clone(&self.builder).add_sub_info(subscription_info)
    }
}

fn parse_pem(input: &str) -> Result<(PartialVapidSignatureBuilder, VapidKeyFormat), VapidKeyError> {
    if let Ok(builder) = VapidSignatureBuilder::from_pem_no_sub(input.as_bytes()) {
        return Ok((builder, VapidKeyFormat::Pem));
    }
    let canonical = canonical_pem(input).ok_or(VapidKeyError::UnrecognisedPrivateKey)?;
    VapidSignatureBuilder::from_pem_no_sub(canonical.as_bytes())
        .map(|builder| (builder, VapidKeyFormat::PemEscaped))
        .map_err(|_| VapidKeyError::UnrecognisedPrivateKey)
}

/// Rebuilds a PEM whose line breaks were lost: the first
/// `-----BEGIN <label>-----` block, body stripped of whitespace and literal
/// `\n`/`\r` escapes, re-wrapped at 64 columns.
fn canonical_pem(input: &str) -> Option<String> {
    const BEGIN: &str = "-----BEGIN ";
    const DASHES: &str = "-----";
    let after_begin = &input[input.find(BEGIN)? + BEGIN.len()..];
    let label_end = after_begin.find(DASHES)?;
    let label = after_begin[..label_end].trim();
    let rest = &after_begin[label_end + DASHES.len()..];
    let end = rest.find(&format!("-----END {label}-----"))?;
    let body: String = rest[..end]
        .replace("\\r", "")
        .replace("\\n", "")
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    if body.is_empty() || !body.is_ascii() {
        return None;
    }
    let mut out = format!("-----BEGIN {label}-----\n");
    for line in body.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(line).ok()?);
        out.push('\n');
    }
    writeln!(out, "-----END {label}-----").ok()?;
    Some(out)
}

fn parse_raw_base64(input: &str) -> Result<PartialVapidSignatureBuilder, VapidKeyError> {
    let normalised = normalise_base64(input);
    // `ES256KeyPair::from_bytes` also takes shorter scalars; a VAPID key is
    // exactly 32 bytes, so anything else is a wrong value, not a key.
    let bytes = URL_SAFE_NO_PAD
        .decode(&normalised)
        .map_err(|_| VapidKeyError::UnrecognisedPrivateKey)?;
    if bytes.len() != 32 {
        return Err(VapidKeyError::UnrecognisedPrivateKey);
    }
    VapidSignatureBuilder::from_base64_no_sub(&normalised)
        .map_err(|_| VapidKeyError::UnrecognisedPrivateKey)
}

/// base64 or base64url, padded or not, to base64url without padding.
fn normalise_base64(input: &str) -> String {
    input
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '=')
        .map(|c| match c {
            '+' => '-',
            '/' => '_',
            c => c,
        })
        .collect()
}

fn decode_base64_lenient(input: &str) -> Option<Vec<u8>> {
    URL_SAFE_NO_PAD.decode(normalise_base64(input)).ok()
}

/// Drops one pair of surrounding `"` or `'`, as left by some `.env`/Secret
/// tooling.
fn strip_matching_quotes(input: &str) -> &str {
    for quote in ['"', '\''] {
        if let Some(inner) = input
            .strip_prefix(quote)
            .and_then(|rest| rest.strip_suffix(quote))
        {
            return inner.trim();
        }
    }
    input
}

#[cfg(test)]
pub(crate) mod tests {
    use base64::engine::general_purpose::STANDARD;

    use super::*;

    /// A throwaway P-256 key generated for this crate's tests (the same
    /// one `send::tests` signs with). Never used anywhere real.
    pub(crate) const TEST_SEC1_PEM: &str = crate::send::tests::TEST_VAPID_PRIVATE_KEY_PEM;

    /// The test key's SEC1 DER: `30 77 02 01 01 04 20 <32-byte scalar>
    /// a0 0a <curve OID> a1 44 03 42 00 <65-byte public key>`.
    fn sec1_der() -> Vec<u8> {
        let body: String = TEST_SEC1_PEM
            .lines()
            .filter(|line| !line.starts_with("-----"))
            .collect();
        STANDARD.decode(body).expect("test PEM body is base64")
    }

    fn scalar() -> Vec<u8> {
        sec1_der()[7..39].to_vec()
    }

    fn expected_public_key() -> Vec<u8> {
        let der = sec1_der();
        der[der.len() - 65..].to_vec()
    }

    fn pem(label: &str, der: &[u8]) -> String {
        let body = STANDARD.encode(der);
        let mut out = format!("-----BEGIN {label}-----\n");
        for line in body.as_bytes().chunks(64) {
            out.push_str(std::str::from_utf8(line).unwrap());
            out.push('\n');
        }
        out + &format!("-----END {label}-----\n")
    }

    /// The same key wrapped as PKCS#8 (RFC 5915 inside RFC 5208), built
    /// from the scalar and public key so no second key is hardcoded.
    fn pkcs8_pem() -> String {
        let mut der = vec![
            0x30, 0x81, 0x87, 0x02, 0x01, 0x00, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce,
            0x3d, 0x02, 0x01, 0x06, 0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, 0x04,
            0x6d, 0x30, 0x6b, 0x02, 0x01, 0x01, 0x04, 0x20,
        ];
        der.extend(scalar());
        der.extend([0xa1, 0x44, 0x03, 0x42, 0x00]);
        der.extend(expected_public_key());
        pem("PRIVATE KEY", &der)
    }

    fn public_key_b64url() -> String {
        URL_SAFE_NO_PAD.encode(expected_public_key())
    }

    fn assert_parses_as(input: &str, format: VapidKeyFormat) {
        let key = VapidKey::parse(input).expect("key must parse");
        assert_eq!(key.format(), format);
        assert_eq!(key.public_key(), expected_public_key());
        key.verify_public_key(&public_key_b64url())
            .expect("derived public key must match");
    }

    #[test]
    fn accepts_sec1_and_pkcs8_pem() {
        assert_parses_as(TEST_SEC1_PEM, VapidKeyFormat::Pem);
        assert_parses_as(&pkcs8_pem(), VapidKeyFormat::Pem);
        // Surrounding whitespace and quotes are not a format change.
        assert_parses_as(&format!("  \"{TEST_SEC1_PEM}\"\n"), VapidKeyFormat::Pem);
    }

    #[test]
    fn accepts_pem_with_literal_backslash_n() {
        for pem in [TEST_SEC1_PEM.to_string(), pkcs8_pem()] {
            assert_parses_as(&pem.replace('\n', "\\n"), VapidKeyFormat::PemEscaped);
            assert_parses_as(&pem.replace('\n', "\\r\\n"), VapidKeyFormat::PemEscaped);
        }
    }

    #[test]
    fn accepts_pem_with_newlines_collapsed_into_spaces() {
        for pem in [TEST_SEC1_PEM.to_string(), pkcs8_pem()] {
            let flattened = pem.trim().replace('\n', " ");
            assert!(!flattened.contains('\n'));
            assert_parses_as(&flattened, VapidKeyFormat::PemEscaped);
        }
    }

    #[test]
    fn accepts_raw_base64url_and_base64_with_or_without_padding() {
        let scalar = scalar();
        for encoded in [
            URL_SAFE_NO_PAD.encode(&scalar),
            base64::engine::general_purpose::URL_SAFE.encode(&scalar),
            STANDARD.encode(&scalar),
            base64::engine::general_purpose::STANDARD_NO_PAD.encode(&scalar),
        ] {
            assert_parses_as(&encoded, VapidKeyFormat::Base64UrlRaw);
        }
    }

    #[test]
    fn a_mismatched_public_key_is_refused() {
        let key = VapidKey::parse(TEST_SEC1_PEM).unwrap();
        // A real P-256 point, but some other key's.
        let other = crate::send::tests::TEST_P256DH;
        assert_eq!(
            key.verify_public_key(other),
            Err(VapidKeyError::PublicKeyMismatch)
        );
        assert_eq!(
            key.verify_public_key("not base64 at all!"),
            Err(VapidKeyError::PublicKeyNotBase64)
        );
        // Padded / standard-alphabet spellings of the right key still match.
        let padded = STANDARD.encode(expected_public_key());
        key.verify_public_key(&padded).unwrap();
    }

    #[test]
    fn garbage_is_refused_without_echoing_the_input() {
        let inputs = [
            "sekrit-garbage-value".to_string(),
            "-----BEGIN EC PRIVATE KEY-----sekritsekrit-----END EC PRIVATE KEY-----".to_string(),
            // 31 bytes: right encoding, wrong length.
            URL_SAFE_NO_PAD.encode([7_u8; 31]),
            // An all-zero scalar is not a valid P-256 private key.
            URL_SAFE_NO_PAD.encode([0_u8; 32]),
        ];
        for input in inputs {
            let err = VapidKey::parse(&input).expect_err("garbage must not parse");
            assert_eq!(err, VapidKeyError::UnrecognisedPrivateKey);
            let message = err.to_string();
            assert!(message.contains("PEM"), "{message}");
            assert!(message.contains("base64url"), "{message}");
            assert!(!message.contains("sekrit"), "error echoed the input");
            assert!(!message.contains(&input), "error echoed the input");
        }
    }

    /// What `send` does with the key for every push: bind it to a
    /// subscription and sign. Covers each accepted format.
    #[test]
    fn every_format_builds_a_send_signature() {
        let subscription = SubscriptionInfo::new(
            "https://push.example.com/endpoint",
            crate::send::tests::TEST_P256DH,
            crate::send::tests::TEST_AUTH,
        );
        for input in [
            TEST_SEC1_PEM.to_string(),
            pkcs8_pem(),
            TEST_SEC1_PEM.replace('\n', "\\n"),
            TEST_SEC1_PEM.trim().replace('\n', " "),
            URL_SAFE_NO_PAD.encode(scalar()),
        ] {
            let key = VapidKey::parse(&input).unwrap();
            let mut builder = key.signature_builder(&subscription);
            builder.add_claim("sub", "mailto:test@example.com");
            let signature = builder.build().expect("signature builds");
            // The `k=` the push service checks against the subscription.
            assert_eq!(signature.auth_k, expected_public_key());
            assert_eq!(signature.auth_t.matches('.').count(), 2, "a JWT");
            // The key is reusable: a second signature from the same parse.
            key.signature_builder(&subscription).build().unwrap();
        }
    }

    #[test]
    fn debug_redacts_the_key() {
        let key = VapidKey::parse(TEST_SEC1_PEM).unwrap();
        assert!(format!("{key:?}").contains("<redacted>"));
    }
}
