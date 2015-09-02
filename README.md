# Bayley
## Introduction
This was a quick proof of concept showing how to extract data from the Bitcoin blockchain and then importing it into Google's Cayley DB.

For more information and spinup instructions, see [The Frontier Group's blog](http://http://blog.thefrontiergroup.com.au/2015/05/blockchain-analytics-with-cayley-db/).

## Spinup instructions

- Install Bitcoin Core
- Install Cayley DB
- Install Pycharm Community
    + Install python-bitcoinlib
    + Install requests
    + Install pprint

## Usage

`python3 bayley.py start`

Process from the very first block

`python3 bayley.py continue`

Get the current maximum block in Cayley DB and then continue extracting from there.

`python3 bayley.py range x y`

Where `x` and `y` are the start and end block heights you want to process.
