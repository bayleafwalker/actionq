# Tranche-4 external path proof bundle

`tranche4-external-paths-v1.tar.gz.base64` materializes the exact bytes of all
51 paths in `external_path_proofs`. It makes byte validation mandatory and
network-independent in CI. Members are named `<consumer_id>/<declared-path>`.

The bundle is bounded to the declared paths only. Appservice secret manifests
are copied exactly as committed and were checked to contain SOPS ciphertext,
not decrypted values. The generator must never invoke `sops`, load deployment
credentials, or include any path absent from the W1 manifest.

To refresh after a separately reviewed pin change:

1. Resolve each non-q-spec proof only from its manifest-declared Git commit;
   resolve q-spec from its three declared host-local files.
2. Materialize exactly `<consumer_id>/<path>` for all 51 records in a fresh
   temporary directory and verify each SHA-256 before packaging.
3. Create the archive with GNU tar using `--sort=name --mtime=@0 --owner=0
   --group=0 --numeric-owner`, gzip it with `gzip -n -9`, then base64-encode it.
4. Update the bundle SHA-256 and run the focused W1 gate. The test rejects
   missing, extra, duplicate, renamed, or byte-corrupted members.

Current decoded bundle SHA-256:
`1c3b37539a5bde36d812fe3a31430216235d0300be95bc875a1f79dfe646307c`.
