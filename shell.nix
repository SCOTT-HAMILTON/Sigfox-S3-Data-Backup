{ pkgs ? import <nixpkgs> {} }:
let
  customPython = pkgs.python3.buildEnv.override {
    extraLibs = with pkgs.python3Packages; [
      requests
      boto3
      h5py
      numpy
      pandas
      packaging
    ];
  };
in
with pkgs; mkShell {
  buildInputs = [
    customPython
  ];
  shellHook = ''
    push_temps(){
      ${customPython}/bin/python3 push_temps.py $@
    }
    download_temps(){
      ${customPython}/bin/python3 download_temps.py $@
    }
    test(){
      ${customPython}/bin/python3 test.py $@
    }
  '';
}

