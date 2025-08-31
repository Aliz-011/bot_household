const ranks = [
    {
        name: 'JAYAPURA',
        ps: 20,
        io: 34,
        rnk: 1
    },
    {
        name: 'AMBON',
        ps: 13,
        io: 21,
        rnk: 2
    },
    {
        name: 'SORONG',
        ps: 10,
        io: 15,
        rnk: 3
    }
]

const ranked = ranks.map(rank => `${rank.rnk === 1 ? '🥇' : rank.rnk === 2 ? '🥈' : rank.rnk === 3 ? '🥉' : '🎖️'} ${rank.name}`).join('\n')

console.log(ranked)
