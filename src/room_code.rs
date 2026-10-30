//! Room codes: six characters a player can read aloud and type, shown as `ABC-DEF`. The alphabet
//! leaves out `I`, `O`, `0` and `1`, which look alike.

const ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const LENGTH: usize = 6;

/// A fresh random code in its shown form.
pub fn generate() -> String {
    let chars: Vec<u8> = (0..LENGTH)
        .map(|_| ALPHABET[rand::random_range(0..ALPHABET.len())])
        .collect();
    shown(&chars)
}

/// The shown form of what a player typed, or `None` when it cannot be a code. Case, spaces and
/// dashes do not matter.
pub fn normalise(typed: &str) -> Option<String> {
    let chars: Vec<u8> = typed
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '-')
        .map(|c| u8::try_from(c.to_ascii_uppercase()).unwrap_or(0))
        .collect();
    let valid = chars.len() == LENGTH && chars.iter().all(|c| ALPHABET.contains(c));
    valid.then(|| shown(&chars))
}

fn shown(chars: &[u8]) -> String {
    let (left, right) = chars.split_at(LENGTH / 2);
    format!(
        "{}-{}",
        String::from_utf8_lossy(left),
        String::from_utf8_lossy(right)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_generated_code_reads_back_as_itself() {
        for _ in 0..100 {
            let code = generate();
            assert_eq!(normalise(&code).as_deref(), Some(code.as_str()));
        }
    }

    #[test]
    fn case_spaces_and_dashes_are_ignored() {
        assert_eq!(normalise(" abc def ").as_deref(), Some("ABC-DEF"));
        assert_eq!(normalise("a-b-c-d-e-f").as_deref(), Some("ABC-DEF"));
    }

    #[test]
    fn lookalikes_wrong_lengths_and_other_characters_are_not_codes() {
        for typed in [
            "ABC-DE0", "ABC-DEI", "ABC-DE", "ABC-DEFG", "", "ÅBC-DEF", "ABC_DEF", "ŁBC-DEF",
        ] {
            assert_eq!(normalise(typed), None, "{typed}");
        }
    }
}
