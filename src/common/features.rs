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
