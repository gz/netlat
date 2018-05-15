extern crate cc;

fn main() {
    cc::Build::new()
        .file("src/hwtstamp.c")
        .compile("hwtstamp.a");
}
