{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = with pkgs; [
    (python311.withPackages (ps: with ps; [
      websockets
      websocket-client
      requests
      redis
      psycopg2
      jupyterlab
      tqdm
      ipywidgets
      beautifulsoup4
      pandas
      pyarrow
      scikitlearn
    ]))
  ];
}
