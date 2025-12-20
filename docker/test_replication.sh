set -e

# docker compose up
nix_node=gachix-compose-nix-node-1
ubuntu_node=gachix-compose-ubuntu-node-1

docker exec -d "$nix_node" nix daemon
docker exec "$nix_node" /bin/sh -c './gachix add $(nix build github:NixOS/nixpkgs/0e1b3164154eba76eb15110e9ad58e86e026f466#hello --print-out-paths)'

docker exec "$ubuntu_node" /bin/sh -c 'GACHIX__STORE__USE_LOCAL_NIX_DAEMON=0 GACHIX__STORE__remotes=ssh://gachix-nix/gachix/cache ./gachix add /nix/store/jrq3p609i85jsg27mr5zxm2imk3mjzyk-hello-2.12.2'
