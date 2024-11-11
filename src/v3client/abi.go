package v3client

const SWAP_EVENT_ABI = `[{
    "anonymous": false,
    "inputs": [
        {
            "indexed": true,
            "internalType": "address",
            "name": "sender",
            "type": "address"
        },
        {
            "indexed": true,
            "internalType": "address",
            "name": "recipient",
            "type": "address"
        },
        {
            "indexed": false,
            "internalType": "int256",
            "name": "amount0",
            "type": "int256"
        },
        {
            "indexed": false,
            "internalType": "int256",
            "name": "amount1",
            "type": "int256"
        },
        {
            "indexed": false,
            "internalType": "uint160",
            "name": "sqrtPriceX96",
            "type": "uint160"
        },
        {
            "indexed": false,
            "internalType": "uint128",
            "name": "liquidity",
            "type": "uint128"
        },
        {
            "indexed": false,
            "internalType": "int24",
            "name": "tick",
            "type": "int24"
        }
    ],
    "name": "Swap",
    "type": "event"
}]`
