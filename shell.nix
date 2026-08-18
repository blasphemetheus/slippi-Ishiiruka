# Build environment for the ExiAI headless dolphin on NixOS.
#   nix-shell --run './build-linux.sh'   (or the cmake/make invocation)
{ pkgs ? import <nixpkgs> { } }:

pkgs.mkShell {
  nativeBuildInputs = with pkgs; [
    cmake
    gnumake
    pkg-config
    git
    cargo
    rustc
  ];

  buildInputs = with pkgs; [
    alsa-lib # slippi-rust-extensions jukebox (alsa-sys)
    bluez
    curl
    enet
    libao
    libevdev
    libGL
    libpng
    libusb1
    lzo
    miniupnpc
    openal
    portaudio
    soundtouch
    systemdLibs # libudev for hidapi
    zlib
    xorg.libX11
    xorg.libXext
    xorg.libXi
    xorg.libXrandr
    xorg.xorgproto
  ];
}
