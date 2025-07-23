/// Check if the current build is in nextgen mode
pub fn is_nextgen_mode() -> bool {
    #[cfg(feature = "nextgen")]
    {
        true
    }
    #[cfg(not(feature = "nextgen"))]
    {
        false
    }
}

#[allow(dead_code)]
pub fn is_legacy_mode() -> bool {
    !is_nextgen_mode()
} 