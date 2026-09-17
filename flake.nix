{
  description = "Development, benchmarking, and profiling environment for serde_arrow";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { nixpkgs, ... }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      devShells = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        {
          default = pkgs.mkShell {
            packages = with pkgs;
              [
                cargo
                clippy
                git
                gnuplot
                python3
                rustc
                rustfmt
              ]
              ++ lib.optionals stdenv.hostPlatform.isLinux [
                flamegraph
                heaptrack
                perf
              ];
          };
        });
    };
}
