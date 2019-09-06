FROM nixos/nix

RUN nix-channel --add https://nixos.org/channels/nixpkgs-unstable nixpkgs
RUN nix-channel --update
RUN nix-env -i gnumake git
RUN nix-env -iA nixpkgs.openjdk nixpkgs.maven
RUN nix-store --gc

WORKDIR /app
