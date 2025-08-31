import TelegramBot from "node-telegram-bot-api";
import { config } from "dotenv"
import * as mysql from 'mysql2/promise'
import { google } from "googleapis";
import cron from 'node-cron'
import { format, subHours, startOfDay, endOfDay } from "date-fns";
import { HttpsProxyAgent } from 'https-proxy-agent'
import fs from 'fs'
import path from 'path'
import { generateSalesTable, calculatePercentage, type RowData } from "./lib/utils";

config({ path: ".env" })

const proxyAgent = new HttpsProxyAgent('http://10.59.105.207:8080')

const token = process.env.TELEGRAM_BOT_TOKEN!;
const bot = new TelegramBot(token, {
    polling: true,
});

// const bot = new TelegramBot(token, {
//     polling: true,
//     request: {
//         agent: proxyAgent,
//         url: `https://api.telegram.org/bot${process.env.TELEGRAM_BOT_TOKEN}/getMe`
//     }
// });

const auth = new google.auth.GoogleAuth({
    keyFile: './cedar-kiln-460702-f0-8e6f032914b1.json',
    scopes: ['https://www.googleapis.com/auth/spreadsheets']
})

const SHEET_ID = process.env.SHEET_ID as string
const TARGET_CHAT_IDS = process.env.TELEGRAM_CHAT_IDS || '1515251812';

// MySQL connection pool configuration
const access: mysql.PoolOptions = {
    user: process.env.DB_USER,
    database: process.env.DB_NAME,
    password: process.env.DB_PASS,
    host: process.env.DB_HOST,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
};

// Create MySQL connection pool
const pool = mysql.createPool(access);

async function executeQuery(query: string, values?: any[]) {
    try {
        const conn = await pool.getConnection();
        const [rows] = await conn.execute(query, values);
        conn.release();
        return rows;
    } catch (error) {
        console.error('Error executing query:', error);
        throw error;
    }
}

async function getValuesFromSheet(range: string) {
    const sheet = google.sheets({ version: 'v4', auth })
    const spreadsheetId = SHEET_ID

    try {
        const response = await sheet.spreadsheets.values.get({
            spreadsheetId,
            range
        })
        const rows = response.data.values

        return rows
    } catch (error) {
        console.error(error)
        return error
    }
}

async function getTargetPs() {
    const rows = await getValuesFromSheet('Sheet1!B10:E24') as any[][]
    const rows2 = await getValuesFromSheet('Sheet1!BE10:BH24') as any[][]

    const results: {
        name: string;
        daily: number;
        mtd: number;
        fm: number;
    }[] = [];

    // The data for branches and their targets starts at index 2 of the returned array,
    // which corresponds to row 12 in the spreadsheet.
    for (let i = 0; i < rows.length; i++) {
        const row = rows[i] as any[];

        // Check if the first column (the branch name) is not empty
        if (row[0] && row[0].trim() !== '') {
            const branchName = row[0] as string;
            const dailyTarget = Number(row[1]) || 0;
            const mtdTarget = Number(row[2]) || 0;
            const fmTarget = Number(row[3]) || 0;

            const existingBranch = results.find(t => t.name === branchName)

            if (existingBranch) {
                existingBranch.daily += dailyTarget
                existingBranch.mtd += mtdTarget
                existingBranch.fm += fmTarget
            } else {
                results.push({
                    name: branchName,
                    daily: dailyTarget,
                    mtd: mtdTarget,
                    fm: fmTarget
                })
            }

        }
    }

    for (let i = 0; i < rows2.length; i++) {
        const row = rows2[i] as any[];

        // Check if the first column (the branch name) is not empty
        if (row[0] && row[0].trim() !== '') {
            const branchName = row[0] as string;
            const dailyTarget = Number(row[1]) || 0;
            const mtdTarget = Number(row[2]) || 0;
            const fmTarget = Number(row[3]) || 0;

            const existingBranch = results.find(t => t.name === branchName)

            if (existingBranch) {
                existingBranch.daily += dailyTarget
                existingBranch.mtd += mtdTarget
                existingBranch.fm += fmTarget
            } else {
                results.push({
                    name: branchName,
                    daily: dailyTarget,
                    mtd: mtdTarget,
                    fm: fmTarget
                })
            }

        }
    }

    return results
}

async function getTargetIo() {
    const rows = await getValuesFromSheet('Sheet1!B29:E43') as any[][]
    const rows2 = await getValuesFromSheet('Sheet1!BE29:BH43') as any[][]

    const results: {
        name: string;
        daily: number;
        mtd: number;
        fm: number;
    }[] = [];

    // The data for branches and their targets starts at index 2 of the returned array,
    // which corresponds to row 12 in the spreadsheet.
    for (let i = 0; i < rows.length; i++) {
        const row = rows[i] as any[];

        // Check if the first column (the branch name) is not empty
        if (row[0] && row[0].trim() !== '') {
            const branchName = row[0] as string;
            const dailyTarget = Number(row[1]) || 0;
            const mtdTarget = Number(row[2]) || 0;
            const fmTarget = Number(row[3]) || 0;

            const existingBranch = results.find(t => t.name === branchName)

            if (existingBranch) {
                existingBranch.daily += dailyTarget
                existingBranch.mtd += mtdTarget
                existingBranch.fm += fmTarget
            } else {
                results.push({
                    name: branchName,
                    daily: dailyTarget,
                    mtd: mtdTarget,
                    fm: fmTarget
                })
            }

        }
    }

    for (let i = 0; i < rows2.length; i++) {
        const row = rows2[i] as any[];

        // Check if the first column (the branch name) is not empty
        if (row[0] && row[0].trim() !== '') {
            const branchName = row[0] as string;
            const dailyTarget = Number(row[1]) || 0;
            const mtdTarget = Number(row[2]) || 0;
            const fmTarget = Number(row[3]) || 0;

            const existingBranch = results.find(t => t.name === branchName)

            if (existingBranch) {
                existingBranch.daily += dailyTarget
                existingBranch.mtd += mtdTarget
                existingBranch.fm += fmTarget
            } else {
                results.push({
                    name: branchName,
                    daily: dailyTarget,
                    mtd: mtdTarget,
                    fm: fmTarget
                })
            }

        }
    }

    return results
}

// Function to fetch data and send message
async function sendScheduledMessage(chatId: string) {
    const currentTime = new Date();
    const startTime = format(startOfDay(currentTime), 'yyyy-MM-dd HH:mm:ss'); // 1 hours ago
    const endTime = format(subHours(currentTime, 2), 'yyyy-MM-dd HH:mm:ss'); // Current time
    const endOfDayTime = format(endOfDay(currentTime), 'yyyy-MM-dd HH:mm:ss'); // End of the day
    const period = format(currentTime, 'yyyyMM')

    try {
        const queryWok = `
            WITH ranked AS (
                SELECT
                    wok,
                    COUNT(CASE WHEN ps_ts BETWEEN ? AND ? THEN 1 END) ps,
                    COUNT(CASE WHEN io_ts BETWEEN ? AND ? THEN 1 END) io,
                    COUNT(CASE WHEN re_ts BETWEEN ? AND ? THEN 1 END) re,
                    COUNT(CASE WHEN ps_ts BETWEEN ? AND ? AND (product_commercial_name NOT LIKE '%Orbit%' AND product_commercial_name NOT LIKE '%Eznet%' AND product_commercial_name NOT LIKE '%Dynamic%') THEN 1 END) ps_ih,
                    COUNT(CASE WHEN io_ts BETWEEN ? AND ? AND (product_commercial_name NOT LIKE '%Orbit%' AND product_commercial_name NOT LIKE '%Eznet%' AND product_commercial_name NOT LIKE '%Dynamic%') THEN 1 END) io_ih,
                    COUNT(CASE WHEN re_ts BETWEEN ? AND ? AND (product_commercial_name NOT LIKE '%Orbit%' AND product_commercial_name NOT LIKE '%Eznet%' AND product_commercial_name NOT LIKE '%Dynamic%') THEN 1 END) re_ih,
                    COUNT(CASE WHEN ps_ts BETWEEN ? AND ? AND (product_commercial_name LIKE '%Orbit%' OR product_commercial_name LIKE '%Eznet%' OR product_commercial_name LIKE '%Dynamic%') THEN 1 END) ps_ezw,
                    COUNT(CASE WHEN io_ts BETWEEN ? AND ? AND (product_commercial_name LIKE '%Orbit%' OR product_commercial_name LIKE '%Eznet%' OR product_commercial_name LIKE '%Dynamic%') THEN 1 END) io_ezw,
                    COUNT(CASE WHEN re_ts BETWEEN ? AND ? AND (product_commercial_name LIKE '%Orbit%' OR product_commercial_name LIKE '%Eznet%' OR product_commercial_name LIKE '%Dynamic%') THEN 1 END) re_ezw,
                    RANK() OVER (ORDER BY COUNT(*) DESC) AS rnk
                FROM household.ih_ordering_detail_order_new_${period}
                WHERE region = 'MALUKU DAN PAPUA' AND order_type = 'NEW SALES'
                GROUP BY 1
            )
            SELECT
                A.*,
                SUM(B.daily_ps) as target_ps,
                ROUND(
                CASE
                    WHEN B.daily_ps = 0 OR A.ps = 0 THEN 0 
                    ELSE A.ps / SUM(B.daily_ps) * 100 
                END, 1) AS drr_ps,
                SUM(B.daily_io) as target_io,
                ROUND(
                CASE
                    WHEN B.daily_io = 0 OR A.io = 0 THEN 0 
                    ELSE A.io / SUM(B.daily_io) * 100 
                END, 1) AS drr_io
            FROM (
                SELECT 
                    CASE
                        WHEN wok in ('AMBON INNER', 'AMBON OUTER') THEN 'AMBON'
                        WHEN wok IN ('JAYAPURA INNER', 'JAYAPURA OUTER') THEN 'JAYAPURA'
                        WHEN wok in ('MANOKWARI NABIRE', 'SORONG RAJA AMPAT') THEN 'SORONG'
                        WHEN wok in ('MIMIKA', 'MERAUKE') THEN 'TIMIKA'
                    END AS branch,
                    A.*
                FROM ranked A
                
                UNION ALL

                SELECT DISTINCT branch, wok, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
                FROM puma_2025.ref_teritory_household A 
                WHERE regional = 'MALUKU DAN PAPUA' 
                AND NOT EXISTS (SELECT 1 rnk FROM ranked B WHERE A.wok = B.wok)
            ) A
            LEFT JOIN household.target_io_ps_hh B ON A.wok = B.wok AND B.periode = ?
            GROUP BY wok
            ORDER BY branch, wok
        `;

        const queryBranch = `
            WITH ranked AS (
                SELECT
                    branch,
                    COUNT(CASE WHEN ps_ts BETWEEN ? AND ? THEN 1 END) ps,
                    COUNT(CASE WHEN io_ts BETWEEN ? AND ? THEN 1 END) io,
                    COUNT(CASE WHEN re_ts BETWEEN ? AND ? THEN 1 END) re,
                    COUNT(CASE WHEN ps_ts BETWEEN ? AND ? AND (product_commercial_name NOT LIKE '%Orbit%' AND product_commercial_name NOT LIKE '%Eznet%' AND product_commercial_name NOT LIKE '%Dynamic%') THEN 1 END) ps_ih,
                    COUNT(CASE WHEN io_ts BETWEEN ? AND ? AND (product_commercial_name NOT LIKE '%Orbit%' AND product_commercial_name NOT LIKE '%Eznet%' AND product_commercial_name NOT LIKE '%Dynamic%') THEN 1 END) io_ih,
                    COUNT(CASE WHEN re_ts BETWEEN ? AND ? AND (product_commercial_name NOT LIKE '%Orbit%' AND product_commercial_name NOT LIKE '%Eznet%' AND product_commercial_name NOT LIKE '%Dynamic%') THEN 1 END) re_ih,
                    COUNT(CASE WHEN ps_ts BETWEEN ? AND ? AND (product_commercial_name LIKE '%Orbit%' OR product_commercial_name LIKE '%Eznet%' OR product_commercial_name LIKE '%Dynamic%') THEN 1 END) ps_ezw,
                    COUNT(CASE WHEN io_ts BETWEEN ? AND ? AND (product_commercial_name LIKE '%Orbit%' OR product_commercial_name LIKE '%Eznet%' OR product_commercial_name LIKE '%Dynamic%') THEN 1 END) io_ezw,
                    COUNT(CASE WHEN re_ts BETWEEN ? AND ? AND (product_commercial_name LIKE '%Orbit%' OR product_commercial_name LIKE '%Eznet%' OR product_commercial_name LIKE '%Dynamic%') THEN 1 END) re_ezw,
                    RANK() OVER (ORDER BY COUNT(*) DESC) AS rnk
                FROM household.ih_ordering_detail_order_new_${period}
                WHERE region = 'MALUKU DAN PAPUA' AND order_type = 'NEW SALES'
                GROUP BY 1
            )
            SELECT
                A.*,
                SUM(B.daily_ps) as target_ps,
                ROUND(
                CASE
                    WHEN B.daily_ps = 0 OR A.ps = 0 THEN 0 
                    ELSE A.ps / SUM(B.daily_ps) * 100 
                END, 1) AS drr_ps,
                SUM(B.daily_io) as target_io,
                ROUND(
                CASE
                    WHEN B.daily_io = 0 OR A.io = 0 THEN 0 
                    ELSE A.io / SUM(B.daily_io) * 100 
                END, 1) AS drr_io
            FROM (
                SELECT *
                FROM ranked

                UNION ALL
    
                SELECT DISTINCT branch, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
                FROM puma_2025.ref_teritory_household A
                WHERE regional = 'MALUKU DAN PAPUA' 
                    AND NOT EXISTS (SELECT 1 FROM ranked B WHERE A.branch = B.branch)
            ) A
            LEFT JOIN household.target_io_ps_hh B ON A.branch = B.branch AND B.periode = ?
            GROUP BY 1
            ORDER BY branch
        `;

        const rowsWok = (await executeQuery(queryWok, [startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, period])) as any[]
        const rowsBranch = (await executeQuery(queryBranch, [startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, period])) as any[]
        const resultsPs = await getTargetPs()
        const resultsIo = await getTargetIo()

        if (!rowsWok || rowsWok.length === 0 || !rowsBranch || rowsBranch.length === 0) {
            await bot.sendMessage(chatId, `Household Hourly Sales Performance - ${endTime} WIB\n\nNo data found for the time range: ${startTime} to ${endTime}.`, { parse_mode: 'HTML' });
            return;
        }

        const targetPsRegion = resultsPs[0];
        const targetPsBranch = resultsPs.slice(1, 5);
        const targetPsWok = resultsPs.slice(5, 13);
        const targetIoRegion = resultsIo[0];
        const targetIoBranch = resultsIo.slice(1, 5);
        const targetIoWok = resultsIo.slice(5, 13);
        const targetReRegion = resultsPs[0];

        const calculatedSalesBranch = rowsBranch.map(item => {
            const targetPs = targetPsBranch.find(t => t.name === item.branch)
            const targetIo = targetIoBranch.find(t => t.name === item.branch)

            if (targetPs && targetIo && (targetPs.daily > 0 && targetIo.daily > 0)) {
                const drr_ps = (item.ps / targetPs.daily * 100).toFixed(1)
                const drr_io = (item.io / targetIo.daily * 100).toFixed(1)

                return {
                    branch: item.branch,
                    ps: item.ps || 0,
                    io: item.io || 0,
                    re: item.re || 0,
                    ps_ih: item.ps_ih || 0,
                    io_ih: item.io_ih || 0,
                    re_ih: item.re_ih || 0,
                    ps_ezw: item.ps_ezw || 0,
                    io_ezw: item.io_ezw || 0,
                    re_ezw: item.re_ezw || 0,
                    target_ps: targetPs.daily,
                    drr_ps: parseFloat(drr_ps),
                    target_io: targetIo.daily,
                    drr_io: parseFloat(drr_io)
                }
            }

            return {
                branch: item.branch,
                ps: item.ps || 0,
                io: item.io || 0,
                re: item.re || 0,
                ps_ih: item.ps_ih || 0,
                io_ih: item.io_ih || 0,
                re_ih: item.re_ih || 0,
                ps_ezw: item.ps_ezw || 0,
                io_ezw: item.io_ezw || 0,
                re_ezw: item.re_ezw || 0,
                target_ps: 0,
                drr_ps: 0,
                target_io: 0,
                drr_io: 0
            }
        })

        const calculatedSalesWok = rowsWok.map(item => {
            const targetPs = targetPsWok.find(t => t.name === item.wok)
            const targetIo = targetIoWok.find(t => t.name === item.wok)

            if (targetPs && targetIo && (targetPs.daily > 0 && targetIo.daily > 0)) {
                const drr_ps = (item.ps / targetPs.daily * 100).toFixed(1)
                const drr_io = (item.io / targetIo.daily * 100).toFixed(1)

                return {
                    wok: item.wok,
                    ps: item.ps || 0,
                    io: item.io || 0,
                    re: item.re || 0,
                    ps_ih: item.ps_ih || 0,
                    io_ih: item.io_ih || 0,
                    re_ih: item.re_ih || 0,
                    ps_ezw: item.ps_ezw || 0,
                    io_ezw: item.io_ezw || 0,
                    re_ezw: item.re_ezw || 0,
                    target_ps: targetPs.daily,
                    drr_ps: parseFloat(drr_ps),
                    target_io: targetIo.daily,
                    drr_io: parseFloat(drr_io)
                }
            }

            return {
                wok: item.wok,
                ps: item.ps || 0,
                io: item.io || 0,
                re: item.re || 0,
                ps_ih: item.ps_ih || 0,
                io_ih: item.io_ih || 0,
                re_ih: item.re_ih || 0,
                ps_ezw: item.ps_ezw || 0,
                io_ezw: item.io_ezw || 0,
                re_ezw: item.re_ezw || 0,
                target_ps: 0,
                drr_ps: 0,
                target_io: 0,
                drr_io: 0
            }
        })

        const totalPs = calculatedSalesBranch.reduce((sum, row) => sum + (row.ps || 0), 0)
        const totalIo = calculatedSalesBranch.reduce((sum, row) => sum + (row.io || 0), 0)
        const totalTargetIo = resultsIo[0]!.daily || 0
        const totalTargetPs = resultsPs[0]!.daily || 0
        const drr_io = totalTargetIo != 0 || totalIo != 0 ? (totalIo / totalTargetIo * 100).toFixed(1) : '0'
        const drr_ps = totalTargetPs != 0 || totalPs != 0 ? (totalPs / totalTargetPs * 100).toFixed(1) : '0'

        const totalReBranch = calculatedSalesBranch.reduce((sum, row) => sum + (row.re || 0), 0);
        const totalRe = totalReBranch

        const totalReSalesIhBranch = calculatedSalesBranch.reduce((sum, row) => sum + (row.re_ih || 0), 0);
        const totalReSalesIh = totalReSalesIhBranch

        const totalReEzwBranch = calculatedSalesBranch.reduce((sum, row) => sum + (row.re_ezw || 0), 0);
        const totalReEzw = totalReEzwBranch

        const totalTargetRe = Math.floor(targetReRegion!.daily / 0.75);

        const dataBranch: RowData[] = calculatedSalesBranch.map(item => ({
            territory: item.branch,
            io_ih6k: item.target_io.toString(),
            io_ezw: item.io_ezw.toString(),
            io_sales_ih: item.io_ih.toString(),
            io_ih6k_ach: item.drr_io + '%',
            io_total: item.io.toString(),
            re_ih6k: Math.floor(item.target_ps / 0.75).toString(),
            re_ezw: item.re_ezw.toString(),
            re_sales_ih: item.re_ih.toString(),
            re_total: item.re.toString(),
            re_ih6k_ach: calculatePercentage(item.re, (item.target_ps / 0.75)), // Fixed
            ps_ih6k: item.target_ps.toString(),
            ps_ezw: item.ps_ezw.toString(),
            ps_sales_ih: item.ps_ih.toString(),
            ps_total: item.ps.toString(),
            ps_ach_ih6k: item.drr_ps + '%',
            ps_io_ps: calculatePercentage(item.ps, item.io), // Fixed
            ps_re_ps: calculatePercentage(item.ps, item.re) // Fixed
        }))
        const dataWok: RowData[] = calculatedSalesWok.map(item => ({
            territory: item.wok,
            io_ih6k: item.target_io.toString(),
            io_ezw: item.io_ezw.toString(),
            io_sales_ih: item.io_ih.toString(),
            io_ih6k_ach: item.drr_io + '%',
            io_total: item.io.toString(),
            re_ih6k: Math.floor(item.target_ps / 0.75).toString(),
            re_ezw: item.re_ezw.toString(),
            re_sales_ih: item.re_ih.toString(),
            re_total: item.re.toString(),
            re_ih6k_ach: calculatePercentage(item.re, (item.target_ps / 0.75)), // Fixed
            ps_ih6k: item.target_ps.toString(),
            ps_ezw: item.ps_ezw.toString(),
            ps_sales_ih: item.ps_ih.toString(),
            ps_total: item.ps.toString(),
            ps_ach_ih6k: item.drr_ps + '%',
            ps_io_ps: calculatePercentage(item.ps, item.io), // Fixed
            ps_re_ps: calculatePercentage(item.ps, item.re) // Fixed
        }))

        const data: RowData[] = [
            {
                territory: 'PUMA',
                io_ih6k: targetIoRegion!.daily.toString(),
                io_ezw: calculatedSalesBranch.reduce((sum, row) => sum + (row.io_ezw || 0), 0).toString(),
                io_sales_ih: calculatedSalesBranch.reduce((sum, row) => sum + (row.io_ih || 0), 0).toString(),
                io_total: (totalIo).toString(),
                io_ih6k_ach: calculatePercentage(totalIo, targetIoRegion!.daily),
                re_ih6k: (totalTargetRe).toString(),
                re_ezw: totalReEzw.toString(),
                re_sales_ih: totalReSalesIh.toString(),
                re_total: totalRe.toString(),
                re_ih6k_ach: calculatePercentage(totalRe, totalTargetRe),
                ps_ih6k: targetPsRegion!.daily.toString(),
                ps_ezw: calculatedSalesBranch.reduce((sum, row) => sum + (row.ps_ezw || 0), 0).toString(),
                ps_sales_ih: calculatedSalesBranch.reduce((sum, row) => sum + (row.ps_ih || 0), 0).toString(),
                ps_total: totalPs.toString(),
                ps_ach_ih6k: calculatePercentage(totalPs, targetPsRegion!.daily),
                ps_io_ps: calculatePercentage(totalPs, totalIo),
                ps_re_ps: calculatePercentage(totalPs, totalRe)
            },
            { territory: 'BRANCH', io_ih6k: '', io_ezw: '', io_sales_ih: '', io_total: '', io_ih6k_ach: '', re_ih6k: '', re_ezw: '', re_sales_ih: '', re_total: '', re_ih6k_ach: '', ps_ih6k: '', ps_ezw: '', ps_ach_ih6k: '', ps_io_ps: '', ps_re_ps: '', ps_total: '', ps_sales_ih: '' },
            ...dataBranch,
            { territory: 'WOK', io_ih6k: '', io_ezw: '', io_sales_ih: '', io_total: '', io_ih6k_ach: '', re_ih6k: '', re_ezw: '', re_sales_ih: '', re_total: '', re_ih6k_ach: '', ps_ih6k: '', ps_ezw: '', ps_ach_ih6k: '', ps_io_ps: '', ps_re_ps: '', ps_total: '', ps_sales_ih: '' },
            ...dataWok
        ]

        const imageBuffer = generateSalesTable(data, format(endTime, 'yyyy-MM-dd'), format(endTime, 'HH:mm:ss'))
        const tempPath = path.join(__dirname + '/images/', 'temp_sales.png');
        fs.writeFileSync(tempPath, imageBuffer)

        const ioToPs = totalPs != 0 || totalIo != 0 ? (totalPs / totalIo * 100).toFixed(2) : '0'

        const formattedPsBranch = calculatedSalesBranch.sort((a, b) => b.drr_ps - a.drr_ps).map((row, idx) => {
            const rank = idx + 1;
            const emoji = rank === 1 ? '🥇' : rank === 2 ? '🥈' : rank === 3 ? '🥉' : '🔹';
            return `${emoji} ${row.branch} (${row.target_ps} | ${row.ps} | ${row.drr_ps}%)`
        }).join('\n')
        const formattedIoBranch = calculatedSalesBranch.sort((a, b) => b.drr_io - a.drr_io).map((row, idx) => {
            const rank = idx + 1;
            const emoji = rank === 1 ? '🥇' : rank === 2 ? '🥈' : rank === 3 ? '🥉' : '🔹'
            return `${emoji} ${row.branch} (${row.target_io} | ${row.io} | ${row.drr_io}%)`
        }).join('\n')
        const formattedPsWok = calculatedSalesWok.sort((a, b) => b.drr_ps - a.drr_ps).map((row, idx) => {
            const rank = idx + 1;
            const emoji = rank === 1 ? '🥇' : rank === 2 ? '🥈' : rank === 3 ? '🥉' : '🔹'
            return `${emoji} ${row.wok} (${row.target_ps} | ${row.ps} | ${row.drr_ps}%)`
        }).join('\n')
        const formattedIoWok = calculatedSalesWok.sort((a, b) => b.drr_io - a.drr_io).map((row, idx) => {
            const rank = idx + 1;
            const emoji = rank === 1 ? '🥇' : rank === 2 ? '🥈' : rank === 3 ? '🥉' : '🔹'
            return `${emoji} ${row.wok} (${row.target_io} | ${row.io} | ${row.drr_io}%)`
        }).join('\n')

        const message = `
<pre><code>📊 Household Hourly Sales Performance
🗓 ${endTime} WIB\n
<b>═════ 🔥IO PERFORMANCE ═════</b>
<b>📌 PUMA Today : ${totalIo} IO (${totalTargetIo} | ${totalIo} | ${drr_io}%)</b>\n
<b>🏢 Branch RANK (Trg | Act | %Ach)</b>
${formattedIoBranch}\n
<b>🌍 WOK RANK (Trg | Act | %Ach)</b>
${formattedIoWok}\n
<b>═════ ⚡️PS PERFORMANCE ═════</b>
<b>📌 PUMA : ${totalPs} PS (${totalTargetPs} | ${totalPs} | ${drr_ps}%)</b>
<b>🔐 PUMA IOtoPS : ${ioToPs}%</b>\n
<b>🏢 Branch RANK(Trg | Act | %Ach)</b>
${formattedPsBranch}\n
<b>🌍 WOK RANK (Trg | Act | %Ach)</b>
${formattedPsWok}\n</code></pre>
`;

        const ids = chatId.split(',')
        const sentTo: string[] = []

        for (const id of ids) {
            if (!id || sentTo.includes(id)) continue;

            try {
                const threadId = id.includes('/') ? id.split('/')[1] : undefined;
                if (threadId) {
                    const photoMsg = await bot.sendPhoto(id, imageBuffer, { parse_mode: 'HTML', message_thread_id: parseInt(threadId) })
                    await bot.sendMessage(id, message, { parse_mode: 'HTML', reply_to_message_id: photoMsg.message_id, message_thread_id: parseInt(threadId) })
                    sentTo.push(id)
                    return
                }
                const photoMsg = await bot.sendPhoto(id, imageBuffer, { parse_mode: 'HTML' })
                await bot.sendMessage(id, message, { parse_mode: 'HTML', reply_to_message_id: photoMsg.message_id })
                sentTo.push(id)
            } catch (error) {
                console.error("Error sending message to chatId " + id + ": ", error);
            }
        }

        console.log(`Scheduled message sent to chat ${chatId} at ${new Date().toISOString()}`);
    } catch (error) {
        console.error('Error in scheduled message:', error);
        await bot.sendMessage(chatId, 'Error fetching data from database.', { parse_mode: 'HTML' });
    }
}

async function testMessage(chatId: string) {
    const currentTime = new Date();
    const startTime = format(startOfDay(currentTime), 'yyyy-MM-dd HH:mm:ss'); // 1 hours ago
    const endTime = format(subHours(currentTime, 2), 'yyyy-MM-dd HH:mm:ss'); // Current time
    const endOfDayTime = format(endOfDay(currentTime), 'yyyy-MM-dd HH:mm:ss'); // End of the day
    const period = format(currentTime, 'yyyyMM')

    try {
        const queryWok = `
            WITH ranked AS (
                SELECT
                    wok,
                    COUNT(CASE WHEN ps_ts BETWEEN ? AND ? THEN 1 END) ps,
                    COUNT(CASE WHEN io_ts BETWEEN ? AND ? THEN 1 END) io,
                    COUNT(CASE WHEN re_ts BETWEEN ? AND ? THEN 1 END) re,
                    COUNT(CASE WHEN ps_ts BETWEEN ? AND ? AND (product_commercial_name NOT LIKE '%Orbit%' AND product_commercial_name NOT LIKE '%Eznet%' AND product_commercial_name NOT LIKE '%Dynamic%') THEN 1 END) ps_ih,
                    COUNT(CASE WHEN io_ts BETWEEN ? AND ? AND (product_commercial_name NOT LIKE '%Orbit%' AND product_commercial_name NOT LIKE '%Eznet%' AND product_commercial_name NOT LIKE '%Dynamic%') THEN 1 END) io_ih,
                    COUNT(CASE WHEN re_ts BETWEEN ? AND ? AND (product_commercial_name NOT LIKE '%Orbit%' AND product_commercial_name NOT LIKE '%Eznet%' AND product_commercial_name NOT LIKE '%Dynamic%') THEN 1 END) re_ih,
                    COUNT(CASE WHEN ps_ts BETWEEN ? AND ? AND (product_commercial_name LIKE '%Orbit%' OR product_commercial_name LIKE '%Eznet%' OR product_commercial_name LIKE '%Dynamic%') THEN 1 END) ps_ezw,
                    COUNT(CASE WHEN io_ts BETWEEN ? AND ? AND (product_commercial_name LIKE '%Orbit%' OR product_commercial_name LIKE '%Eznet%' OR product_commercial_name LIKE '%Dynamic%') THEN 1 END) io_ezw,
                    COUNT(CASE WHEN re_ts BETWEEN ? AND ? AND (product_commercial_name LIKE '%Orbit%' OR product_commercial_name LIKE '%Eznet%' OR product_commercial_name LIKE '%Dynamic%') THEN 1 END) re_ezw,
                    RANK() OVER (ORDER BY COUNT(*) DESC) AS rnk
                FROM household.ih_ordering_detail_order_new_${period}
                WHERE region = 'MALUKU DAN PAPUA' AND order_type = 'NEW SALES'
                GROUP BY 1
            )
            SELECT
                A.*,
                SUM(B.daily_ps) as target_ps,
                ROUND(
                CASE
                    WHEN B.daily_ps = 0 OR A.ps = 0 THEN 0 
                    ELSE A.ps / SUM(B.daily_ps) * 100 
                END, 1) AS drr_ps,
                SUM(B.daily_io) as target_io,
                ROUND(
                CASE
                    WHEN B.daily_io = 0 OR A.io = 0 THEN 0 
                    ELSE A.io / SUM(B.daily_io) * 100 
                END, 1) AS drr_io
            FROM (
                SELECT *
                FROM ranked
                
                UNION ALL

                SELECT DISTINCT wok, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
                FROM puma_2025.ref_teritory_household A 
                WHERE regional = 'MALUKU DAN PAPUA' 
                AND NOT EXISTS (SELECT 1 rnk FROM ranked B WHERE A.wok = B.wok)
            ) A
            LEFT JOIN household.target_io_ps_hh B ON A.wok = B.wok AND B.periode = ?
            GROUP BY 1
        `;

        const queryBranch = `
            WITH ranked AS (
                SELECT
                    branch,
                    COUNT(CASE WHEN ps_ts BETWEEN ? AND ? THEN 1 END) ps,
                    COUNT(CASE WHEN io_ts BETWEEN ? AND ? THEN 1 END) io,
                    COUNT(CASE WHEN re_ts BETWEEN ? AND ? THEN 1 END) re,
                    COUNT(CASE WHEN ps_ts BETWEEN ? AND ? AND (product_commercial_name NOT LIKE '%Orbit%' AND product_commercial_name NOT LIKE '%Eznet%' AND product_commercial_name NOT LIKE '%Dynamic%') THEN 1 END) ps_ih,
                    COUNT(CASE WHEN io_ts BETWEEN ? AND ? AND (product_commercial_name NOT LIKE '%Orbit%' AND product_commercial_name NOT LIKE '%Eznet%' AND product_commercial_name NOT LIKE '%Dynamic%') THEN 1 END) io_ih,
                    COUNT(CASE WHEN re_ts BETWEEN ? AND ? AND (product_commercial_name NOT LIKE '%Orbit%' AND product_commercial_name NOT LIKE '%Eznet%' AND product_commercial_name NOT LIKE '%Dynamic%') THEN 1 END) re_ih,
                    COUNT(CASE WHEN ps_ts BETWEEN ? AND ? AND (product_commercial_name LIKE '%Orbit%' OR product_commercial_name LIKE '%Eznet%' OR product_commercial_name LIKE '%Dynamic%') THEN 1 END) ps_ezw,
                    COUNT(CASE WHEN io_ts BETWEEN ? AND ? AND (product_commercial_name LIKE '%Orbit%' OR product_commercial_name LIKE '%Eznet%' OR product_commercial_name LIKE '%Dynamic%') THEN 1 END) io_ezw,
                    COUNT(CASE WHEN re_ts BETWEEN ? AND ? AND (product_commercial_name LIKE '%Orbit%' OR product_commercial_name LIKE '%Eznet%' OR product_commercial_name LIKE '%Dynamic%') THEN 1 END) re_ezw,
                    RANK() OVER (ORDER BY COUNT(*) DESC) AS rnk
                FROM household.ih_ordering_detail_order_new_${period}
                WHERE region = 'MALUKU DAN PAPUA' AND order_type = 'NEW SALES'
                GROUP BY 1
            )
            SELECT
                A.*,
                SUM(B.daily_ps) as target_ps,
                ROUND(
                CASE
                    WHEN B.daily_ps = 0 OR A.ps = 0 THEN 0 
                    ELSE A.ps / SUM(B.daily_ps) * 100 
                END, 1) AS drr_ps,
                SUM(B.daily_io) as target_io,
                ROUND(
                CASE
                    WHEN B.daily_io = 0 OR A.io = 0 THEN 0 
                    ELSE A.io / SUM(B.daily_io) * 100 
                END, 1) AS drr_io
            FROM (
                SELECT *
                FROM ranked

                UNION ALL
    
                SELECT DISTINCT branch, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
                FROM puma_2025.ref_teritory_household A
                WHERE regional = 'MALUKU DAN PAPUA' 
                    AND NOT EXISTS (SELECT 1 FROM ranked B WHERE A.branch = B.branch)
            ) A
            LEFT JOIN household.target_io_ps_hh B ON A.branch = B.branch AND B.periode = ?
            GROUP BY 1
        `;

        const rowsWok = (await executeQuery(queryWok, [startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, period])) as any[]
        const rowsBranch = (await executeQuery(queryBranch, [startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, startTime, endOfDayTime, period])) as any[]
        const resultsPs = await getTargetPs()
        const resultsIo = await getTargetIo()

        if (!rowsWok || rowsWok.length === 0 || !rowsBranch || rowsBranch.length === 0) {
            await bot.sendMessage(chatId, `Household Hourly Sales Performance - ${endTime} WIB\n\nNo data found for the time range: ${startTime} to ${endTime}.`, { parse_mode: 'HTML' });
            return;
        }

        const targetPsRegion = resultsPs[0]
        const targetPsBranch = resultsPs.slice(1, 5)
        const targetPsWok = resultsPs.slice(5, 13)
        const targetIoRegion = resultsIo[0]
        const targetIoBranch = resultsIo.slice(1, 5)
        const targetIoWok = resultsIo.slice(5, 13)
        const targetReRegion = resultsPs[0]

        console.log({ targetPsRegion, targetPsBranch, targetPsWok });


        const calculatedSalesBranch = rowsBranch.map(item => {
            const targetPs = targetPsBranch.find(t => t.name === item.branch)
            const targetIo = targetIoBranch.find(t => t.name === item.branch)

            if (targetPs && targetIo && (targetPs.daily > 0 && targetIo.daily > 0)) {
                const drr_ps = (item.ps / targetPs.daily * 100).toFixed(1)
                const drr_io = (item.io / targetIo.daily * 100).toFixed(1)

                return {
                    branch: item.branch,
                    ps: item.ps || 0,
                    io: item.io || 0,
                    re: item.re || 0,
                    ps_ih: item.ps_ih || 0,
                    io_ih: item.io_ih || 0,
                    re_ih: item.re_ih || 0,
                    ps_ezw: item.ps_ezw || 0,
                    io_ezw: item.io_ezw || 0,
                    re_ezw: item.re_ezw || 0,
                    target_ps: targetPs.daily,
                    drr_ps: parseFloat(drr_ps),
                    target_io: targetIo.daily,
                    drr_io: parseFloat(drr_io)
                }
            }

            return {
                branch: item.branch,
                ps: item.ps || 0,
                io: item.io || 0,
                re: item.re || 0,
                ps_ih: item.ps_ih || 0,
                io_ih: item.io_ih || 0,
                re_ih: item.re_ih || 0,
                ps_ezw: item.ps_ezw || 0,
                io_ezw: item.io_ezw || 0,
                re_ezw: item.re_ezw || 0,
                target_ps: 0,
                drr_ps: 0,
                target_io: 0,
                drr_io: 0
            }
        })

        const calculatedSalesWok = rowsWok.map(item => {
            const targetPs = targetPsWok.find(t => t.name === item.wok)
            const targetIo = targetIoWok.find(t => t.name === item.wok)

            if (targetPs && targetIo && (targetPs.daily > 0 && targetIo.daily > 0)) {
                const drr_ps = (item.ps / targetPs.daily * 100).toFixed(1)
                const drr_io = (item.io / targetIo.daily * 100).toFixed(1)

                return {
                    wok: item.wok,
                    ps: item.ps || 0,
                    io: item.io || 0,
                    re: item.re || 0,
                    ps_ih: item.ps_ih || 0,
                    io_ih: item.io_ih || 0,
                    re_ih: item.re_ih || 0,
                    ps_ezw: item.ps_ezw || 0,
                    io_ezw: item.io_ezw || 0,
                    re_ezw: item.re_ezw || 0,
                    target_ps: targetPs.daily,
                    drr_ps: parseFloat(drr_ps),
                    target_io: targetIo.daily,
                    drr_io: parseFloat(drr_io)
                }
            }

            return {
                wok: item.wok,
                ps: item.ps || 0,
                io: item.io || 0,
                re: item.re || 0,
                ps_ih: item.ps_ih || 0,
                io_ih: item.io_ih || 0,
                re_ih: item.re_ih || 0,
                ps_ezw: item.ps_ezw || 0,
                io_ezw: item.io_ezw || 0,
                re_ezw: item.re_ezw || 0,
                target_ps: 0,
                drr_ps: 0,
                target_io: 0,
                drr_io: 0
            }
        })

        const totalPs = calculatedSalesBranch.reduce((sum, row) => sum + (row.ps || 0), 0)
        const totalIo = calculatedSalesBranch.reduce((sum, row) => sum + (row.io || 0), 0)
        const totalTargetIo = resultsIo[0]!.daily || 0
        const totalTargetPs = resultsPs[0]!.daily || 0
        const drr_io = totalTargetIo != 0 || totalIo != 0 ? (totalIo / totalTargetIo * 100).toFixed(1) : '0'
        const drr_ps = totalTargetPs != 0 || totalPs != 0 ? (totalPs / totalTargetPs * 100).toFixed(1) : '0'

        const totalReBranch = calculatedSalesBranch.reduce((sum, row) => sum + (row.re || 0), 0);
        const totalRe = totalReBranch

        const totalReSalesIhBranch = calculatedSalesBranch.reduce((sum, row) => sum + (row.re_ih || 0), 0);
        const totalReSalesIh = totalReSalesIhBranch

        const totalReEzwBranch = calculatedSalesBranch.reduce((sum, row) => sum + (row.re_ezw || 0), 0);
        const totalReEzw = totalReEzwBranch

        const totalTargetRe = Math.floor(targetReRegion!.daily * 0.75);

        const dataBranch: RowData[] = calculatedSalesBranch.map(item => ({
            territory: item.branch,
            io_ih6k: item.target_io.toString(),
            io_ezw: item.io_ezw.toString(),
            io_sales_ih: item.io_ih.toString(),
            io_ih6k_ach: item.drr_io + '%',
            io_total: item.io.toString(),
            re_ih6k: Math.floor(item.target_ps * 0.75).toString(),
            re_ezw: item.re_ezw.toString(),
            re_sales_ih: item.re_ih.toString(),
            re_total: item.re.toString(),
            re_ih6k_ach: calculatePercentage(item.re, (item.target_ps * 0.75)), // Fixed
            ps_ih6k: item.target_ps.toString(),
            ps_ezw: item.ps_ezw.toString(),
            ps_sales_ih: item.ps_ih.toString(),
            ps_total: item.ps.toString(),
            ps_ach_ih6k: item.drr_ps + '%',
            ps_io_ps: calculatePercentage(item.ps, item.io), // Fixed
            ps_re_ps: calculatePercentage(item.ps, item.re) // Fixed
        }))
        const dataWok: RowData[] = calculatedSalesWok.map(item => ({
            territory: item.wok,
            io_ih6k: item.target_io.toString(),
            io_ezw: item.io_ezw.toString(),
            io_sales_ih: item.io_ih.toString(),
            io_ih6k_ach: item.drr_io + '%',
            io_total: item.io.toString(),
            re_ih6k: Math.floor(item.target_ps * 75 / 100).toString(),
            re_ezw: item.re_ezw.toString(),
            re_sales_ih: item.re_ih.toString(),
            re_total: item.re.toString(),
            re_ih6k_ach: calculatePercentage(item.re, (item.target_ps * 0.75)), // Fixed
            ps_ih6k: item.target_ps.toString(),
            ps_ezw: item.ps_ezw.toString(),
            ps_sales_ih: item.ps_ih.toString(),
            ps_total: item.ps.toString(),
            ps_ach_ih6k: item.drr_ps + '%',
            ps_io_ps: calculatePercentage(item.ps, item.io), // Fixed
            ps_re_ps: calculatePercentage(item.ps, item.re) // Fixed
        }))

        const data: RowData[] = [
            {
                territory: 'PUMA',
                io_ih6k: targetIoRegion!.daily.toString(),
                io_ezw: calculatedSalesBranch.reduce((sum, row) => sum + (row.io_ezw || 0), 0).toString(),
                io_sales_ih: calculatedSalesBranch.reduce((sum, row) => sum + (row.io_ih || 0), 0).toString(),
                io_total: (totalIo).toString(),
                io_ih6k_ach: calculatePercentage(totalIo, targetIoRegion!.daily),
                re_ih6k: (totalTargetRe).toString(),
                re_ezw: totalReEzw.toString(),
                re_sales_ih: totalReSalesIh.toString(),
                re_total: totalRe.toString(),
                re_ih6k_ach: calculatePercentage(totalRe, totalTargetRe),
                ps_ih6k: targetPsRegion!.daily.toString(),
                ps_ezw: calculatedSalesBranch.reduce((sum, row) => sum + (row.ps_ezw || 0), 0).toString(),
                ps_sales_ih: calculatedSalesBranch.reduce((sum, row) => sum + (row.ps_ih || 0), 0).toString(),
                ps_total: totalPs.toString(),
                ps_ach_ih6k: calculatePercentage(totalPs, targetPsRegion!.daily),
                ps_io_ps: calculatePercentage(totalPs, totalIo),
                ps_re_ps: calculatePercentage(totalPs, totalRe)
            },
            { territory: 'BRANCH', io_ih6k: '', io_ezw: '', io_sales_ih: '', io_total: '', io_ih6k_ach: '', re_ih6k: '', re_ezw: '', re_sales_ih: '', re_total: '', re_ih6k_ach: '', ps_ih6k: '', ps_ezw: '', ps_ach_ih6k: '', ps_io_ps: '', ps_re_ps: '', ps_total: '', ps_sales_ih: '' },
            ...dataBranch,
            { territory: 'WOK', io_ih6k: '', io_ezw: '', io_sales_ih: '', io_total: '', io_ih6k_ach: '', re_ih6k: '', re_ezw: '', re_sales_ih: '', re_total: '', re_ih6k_ach: '', ps_ih6k: '', ps_ezw: '', ps_ach_ih6k: '', ps_io_ps: '', ps_re_ps: '', ps_total: '', ps_sales_ih: '' },
            ...dataWok
        ]

        const imageBuffer = generateSalesTable(data, format(endTime, 'yyyy-MM-dd'), format(endTime, 'HH:mm:ss'))
        const tempPath = path.join(__dirname + '/images/', 'temp_sales.png');
        fs.writeFileSync(tempPath, imageBuffer)

        const ioToPs = totalPs != 0 || totalIo != 0 ? (totalPs / totalIo * 100).toFixed(2) : '0'

        const formattedPsBranch = calculatedSalesBranch.sort((a, b) => b.drr_ps - a.drr_ps).map((row, idx) => {
            const rank = idx + 1;
            const emoji = rank === 1 ? '🥇' : rank === 2 ? '🥈' : rank === 3 ? '🥉' : '🔹';
            return `${emoji} ${row.branch} (${row.target_ps} | ${row.ps} | ${row.drr_ps}%)`
        }).join('\n')
        const formattedIoBranch = calculatedSalesBranch.sort((a, b) => b.drr_io - a.drr_io).map((row, idx) => {
            const rank = idx + 1;
            const emoji = rank === 1 ? '🥇' : rank === 2 ? '🥈' : rank === 3 ? '🥉' : '🔹'
            return `${emoji} ${row.branch} (${row.target_io} | ${row.io} | ${row.drr_io}%)`
        }).join('\n')
        const formattedPsWok = calculatedSalesWok.sort((a, b) => b.drr_ps - a.drr_ps).map((row, idx) => {
            const rank = idx + 1;
            const emoji = rank === 1 ? '🥇' : rank === 2 ? '🥈' : rank === 3 ? '🥉' : '🔹'
            return `${emoji} ${row.wok} (${row.target_ps} | ${row.ps} | ${row.drr_ps}%)`
        }).join('\n')
        const formattedIoWok = calculatedSalesWok.sort((a, b) => b.drr_io - a.drr_io).map((row, idx) => {
            const rank = idx + 1;
            const emoji = rank === 1 ? '🥇' : rank === 2 ? '🥈' : rank === 3 ? '🥉' : '🔹'
            return `${emoji} ${row.wok} (${row.target_io} | ${row.io} | ${row.drr_io}%)`
        }).join('\n')

        const message = `
<pre><code>📊 Household Hourly Sales Performance
🗓 ${endTime} WIB\n
<b>═════ 🔥IO PERFORMANCE ═════</b>
<b>📌 PUMA Today : ${totalIo} IO (${totalTargetIo} | ${totalIo} | ${drr_io}%)</b>\n
<b>🏢 Branch RANK (Trg | Act | %Ach)</b>
${formattedIoBranch}\n
<b>🌍 WOK RANK (Trg | Act | %Ach)</b>
${formattedIoWok}\n
<b>═════ ⚡️PS PERFORMANCE ═════</b>
<b>📌 PUMA : ${totalPs} PS (${totalTargetPs} | ${totalPs} | ${drr_ps}%)</b>
<b>🔐 PUMA IOtoPS : ${ioToPs}%</b>\n
<b>🏢 Branch RANK(Trg | Act | %Ach)</b>
${formattedPsBranch}\n
<b>🌍 WOK RANK (Trg | Act | %Ach)</b>
${formattedPsWok}\n</code></pre>
`;

        const ids = chatId.split(',')
        const sentTo: string[] = []

        for (const id of ids) {
            if (!id || sentTo.includes(id)) continue;

            try {
                const threadId = id.includes('/') ? id.split('/')[1] : undefined;
                if (threadId) {
                    const photoMsg = await bot.sendPhoto(id, imageBuffer, { parse_mode: 'HTML', message_thread_id: parseInt(threadId) })
                    await bot.sendMessage(id, message, { parse_mode: 'HTML', reply_to_message_id: photoMsg.message_id, message_thread_id: parseInt(threadId) })
                    // await bot.sendMessage(id, message, { parse_mode: 'HTML', message_thread_id: parseInt(threadId) })
                    sentTo.push(id)
                    return
                }
                const photoMsg = await bot.sendPhoto(id, imageBuffer, { parse_mode: 'HTML' })
                await bot.sendMessage(id, message, { parse_mode: 'HTML', reply_to_message_id: photoMsg.message_id })
                // await bot.sendMessage(id, message, { parse_mode: 'HTML' });
                sentTo.push(id)
            } catch (error) {
                console.error("Error sending message to chatId " + id + ": ", error);
            }
        }

        console.log(`Scheduled message sent to chat ${chatId} at ${new Date().toISOString()}`);
    } catch (error) {
        console.error('Error in scheduled message:', error);
        await bot.sendMessage(chatId, 'Error fetching data from database.', { parse_mode: 'HTML' });
    }
}

bot.onText(/\/start/, async (msg) => {
    const chatId = msg.chat.id;
    bot.sendMessage(chatId, "Hi", { parse_mode: 'HTML' });
});

bot.onText(/\/check_ps/, async (msg) => {
    const chatId = msg.chat.id;
    sendScheduledMessage(chatId.toString());
});

bot.onText(/\/check_id/, async (msg) => {
    const chatId = msg.chat.id;
    const threadId = msg.message_thread_id

    bot.sendMessage(chatId, `${chatId}`, { parse_mode: 'HTML' })
})

// Schedule the message to be sent every 1 hours
cron.schedule('0 10-23 * * *', () => {
    console.log('Running scheduled task at', new Date().toISOString());
    const id = TARGET_CHAT_IDS
    sendScheduledMessage(id);
}, { timezone: 'Asia/Tokyo' });

// Gracefully close the pool on process termination
process.on('SIGTERM', async () => {
    console.log('Closing MySQL connection pool...');
    await pool.end();
    console.log('MySQL connection pool closed.');
    process.exit(0);
});

console.log('Telegram bot running with scheduled messages every 1 hours.');