{ pkgs ? import <nixpkgs> {} }:
let
  selectolax = pkgs.python3Packages.buildPythonPackage rec {
    pname = "selectolax";  # Replace with actual package name
    version = "0.3.25";           # Replace with actual version

    # Fetch from PyPI
    src = pkgs.python3Packages.fetchPypi {
      inherit pname version;
      sha256 = "sha256-Va7jlP6dacgdLG3SRvwhqCKqjQMOPQ3B2S8uj8aLD1o="; # You'll get an error with the correct hash to put here
    };

    # If your package has no tests or you want to skip them
    doCheck = false;
  };
  googlenewsdecoder = pkgs.python3Packages.buildPythonPackage rec {
    pname = "googlenewsdecoder";  # Replace with actual package name
    version = "0.1.6";           # Replace with actual version

    # Fetch from PyPI
    src = pkgs.python3Packages.fetchPypi {
      inherit pname version;
      sha256 = "sha256-M5fXlb9V3SKSQcyVo1Ov8T3PlSXfIis0kzRKqR6dB7s="; # You'll get an error with the correct hash to put here
    };

    # Add any dependencies your package needs
    propagatedBuildInputs = with pkgs.python3Packages; [
      selectolax
    ];

    # If your package has no tests or you want to skip them
    doCheck = false;
  };
in
pkgs.mkShell {
  buildInputs = with pkgs; [
    (python311.withPackages (ps: with ps; [
      aiohttp
      motor
      feedparser
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
      googlenewsdecoder
    ]))
  ];
}
