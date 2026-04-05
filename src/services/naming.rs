pub fn sanitize_filename(name: &str) -> String {
    let mut cleaned = name.trim().to_string();
    for ch in ['/', ':', '\\', '*', '?', '"', '<', '>', '|'] {
        cleaned = cleaned.replace(ch, "_");
    }
    cleaned
}
