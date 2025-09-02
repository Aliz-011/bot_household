import { format } from "date-fns";

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

const currentTime = new Date()
const endDay = format(new Date(currentTime.getFullYear(), currentTime.getMonth(), currentTime.getDate(), 23, 59), 'yyyy-MM-dd HH:mm:ss')

console.log({ endDay: currentTime.getHours() === 10 && currentTime.getMinutes() === 34 });

