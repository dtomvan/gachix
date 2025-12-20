FROM nixos/nix

WORKDIR gachix
COPY . .

RUN echo "experimental-features = nix-command flakes" >> /etc/nix/nix.conf

RUN nix build
RUN cp ./result/bin/gachix .

CMD ["./gachix", "serve"]
