package v2client

const PAIR_ABI = `[
    {
      "constant": true,
      "inputs": [],
      "name": "getReserves",
      "outputs": [
        {
          "internalType": "uint112",
          "name": "reserve0",
          "type": "uint112"
        },
        {
          "internalType": "uint112",
          "name": "reserve1",
          "type": "uint112"
        },
        {
          "internalType": "uint32",
          "name": "blockTimestampLast",
          "type": "uint32"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    }]`

const SYNC_EVENT_ABI = `[{
    "anonymous": false,
    "inputs": [
        {
            "indexed": false,
            "internalType": "uint112",
            "name": "reserve0",
            "type": "uint112"
        },
        {
            "indexed": false,
            "internalType": "uint112",
            "name": "reserve1",
            "type": "uint112"
        }
    ],
    "name": "Sync",
    "type": "event"
}]`
