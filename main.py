import polars as pl
import ctc
import json

data_path = "/home/jay/projects/personal/paradigm/data/ethereum_contracts__v1_0_0__00000000_to_00999999.parquet"


def save_to_json(contracts):
    with open('contracts.json', 'w') as f:
        json.dump(contracts, f)

    print("Saved contracts to contracts.json")


def getContracts():
    column = 'contract_address'
    col = '0x' + (pl.col(column).struct.field(column).bin.encode('hex'))

    contracts = []

    data = (
        pl.scan_parquet(data_path)
        .select(pl.col(column).value_counts())
        .select([col.alias(column)])
        .collect(streaming=True)
    )['contract_address']

    for binary_address in data:
        address = ctc.binary_convert(binary_address, 'prefix_hex')
        contracts.append(address)

    save_to_json(contracts)


getContracts()
