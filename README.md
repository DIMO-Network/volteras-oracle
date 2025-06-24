# Volteras-Oracle
DIMO Connection Oracle for Volteras vendor

### DIMO Oracle

An application that exposes an API and performs data streaming from your data source to a DIMO Node.
The API is used for a frontend to handle onboarding and removal. The data streaming pulls data however you need it to 
from your systems and then forwards it via an http POST to a DIMO Node. This app also does minting of necessary on-chain records. 
This repository is an example in Golang of a DIMO Oracle, which you can base your solution on and just replace necessary parts.

### Structure

Both Backend and Frontend are in the same repo.

Deployed to https://volteras.dimo.zone
