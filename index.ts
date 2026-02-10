import TelegramBot from "node-telegram-bot-api";
import { config } from "dotenv"
import * as mysql from 'mysql2/promise'
import { google } from "googleapis";
import cron from 'node-cron'
import { format, subHours, startOfDay, endOfDay, subDays, isWeekend, startOfMonth } from "date-fns";
import { HttpsProxyAgent } from 'https-proxy-agent'
import fs from 'fs'
import path from 'path'
import { generateSalesTable, calculatePercentage, type RowData } from "./lib/utils";

config({ path: ".env" })

const proxyAgent = new HttpsProxyAgent('http://10.59.105.207:8080')

const token = process.env.TELEGRAM_BOT_TOKEN!;
const bot = new TelegramBot(token, {
    polling: true,
    filepath: false
});

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
    queueLimit: 0,
    timezone: 'local'
};

// Create MySQL connection pool
const pool = mysql.createPool(access);

async function executeQuery(query: string, values?: any[]) {
    try {
        const conn = await pool.getConnection();
        await conn.execute("SET time_zone = '+07:00'")
        const [rows] = await conn.execute(query, values);
        conn.release();
        return rows as any[]
    } catch (error) {
        console.error('Error executing query:', error);
        throw error;
    }
}

function resultsToArray(results: any[], includeHeaders: boolean = true): any[][] {
    if (!results.length) return [];

    const headers = Object.keys(results[0]);
    const data = results.map(row => headers.map(header => row[header] ?? ''));

    return includeHeaders ? [headers, ...data] : data;
}

async function queryToArray(query: string, params?: any[], includeHeaders: boolean = true): Promise<any[][]> {
    const results = await executeQuery(query, params);
    return resultsToArray(results, includeHeaders);
}

async function writeData(range: string, values: any[][]): Promise<any> {
    const sheets = google.sheets({ version: 'v4', auth })
    const spreadsheetId = SHEET_ID

    try {
        const response = await sheets.spreadsheets.values.update({
            spreadsheetId,
            range,
            valueInputOption: 'USER_ENTERED',
            requestBody: { values }
        });
        return response.data;
    } catch (error) {
        console.error('Error writing to sheet:', error);
        throw error;
    }
}

async function clearSheet(range: string): Promise<any> {
    const sheets = google.sheets({ version: 'v4', auth })
    const spreadsheetId = SHEET_ID

    try {
        const response = await sheets.spreadsheets.values.clear({
            spreadsheetId,
            range
        });
        return response.data;
    } catch (error) {
        console.error('Error writing to sheet:', error);
        throw error;
    }
}

// Function to fetch data and send message
async function sendScheduledMessage(chatId: string) {
    const currentTime = new Date();
    const today = currentTime.getHours() === 8 && currentTime.getMinutes() === 15 ? format(subDays(currentTime, 1), 'yyyy-MM-dd') : format(currentTime, 'yyyy-MM-dd')
    const yesterday = subDays(currentTime, 1)
    const startTime = format(startOfDay(currentTime), 'yyyy-MM-dd HH:mm:ss'); // 1 hours ago
    const endTime = format(subHours(currentTime, 2), 'yyyy-MM-dd HH:mm:ss'); // Current time
    const endOfDayTime = format(endOfDay(currentTime), 'yyyy-MM-dd HH:mm:ss'); // End of the day
    const period = format(currentTime, 'yyyy-MM-dd') === format(startOfMonth(currentTime), 'yyyy-MM-dd') && (currentTime.getHours() === 8 && currentTime.getMinutes() === 15) ? format(yesterday, 'yyyyMM') : format(today, 'yyyyMM')
    const flagWeekend = isWeekend(new Date(today)) ? 'weekend' : 'weekdays'

    try {
        const queryWok = `
            WITH ranked AS (
                SELECT
                    branch,
                    wok,
                    sto_co as sto,
                    (COUNT(CASE WHEN DATE_FORMAT(ps_ts, '%Y-%m-%d') = ? THEN 1 END)) ps,
                    (COUNT(CASE WHEN DATE_FORMAT(io_ts, '%Y-%m-%d') = ? THEN 1 END)) io,
                    (COUNT(CASE WHEN DATE_FORMAT(re_ts, '%Y-%m-%d') = ? THEN 1 END)) re,
                    COUNT(CASE WHEN DATE_FORMAT(ps_ts, '%Y-%m-%d') = ? AND (product_commercial_name NOT LIKE '%Orbit%' AND product_commercial_name NOT LIKE '%Eznet%') THEN 1 END) ps_ih,
                    COUNT(CASE WHEN DATE_FORMAT(io_ts, '%Y-%m-%d') = ? AND (product_commercial_name NOT LIKE '%Orbit%' AND product_commercial_name NOT LIKE '%Eznet%') THEN 1 END) io_ih,
                    COUNT(CASE WHEN DATE_FORMAT(re_ts, '%Y-%m-%d') = ? AND (product_commercial_name NOT LIKE '%Orbit%' AND product_commercial_name NOT LIKE '%Eznet%') THEN 1 END) re_ih,
                    COUNT(CASE WHEN DATE_FORMAT(ps_ts, '%Y-%m-%d') = ? AND (product_commercial_name LIKE '%Orbit%' OR product_commercial_name LIKE '%Eznet%') THEN 1 END) ps_ezw,
                    COUNT(CASE WHEN DATE_FORMAT(io_ts, '%Y-%m-%d') = ? AND (product_commercial_name LIKE '%Orbit%' OR product_commercial_name LIKE '%Eznet%') THEN 1 END) io_ezw,
                    COUNT(CASE WHEN DATE_FORMAT(re_ts, '%Y-%m-%d') = ? AND (product_commercial_name LIKE '%Orbit%' OR product_commercial_name LIKE '%Eznet%') THEN 1 END) re_ezw,
                    RANK() OVER (ORDER BY COUNT(*) DESC) AS rnk
                FROM (select * from household.ih_ordering_detail_order_new) a
                WHERE region IN ('MALUKU DAN PAPUA', 'PUMA') AND order_type = 'NEW SALES'
                GROUP BY 1
            )
            SELECT
                X.branch,
                A.wok,
                SUM(ps) ps, SUM(io) io, sum(re) re,
                sum(ps_ih) ps_ih, sum(io_ih) io_ih, SUM(re_ih) re_ih,
                sum(ps_ezw) ps_ezw, sum(io_ezw) io_ezw, SUM(re_ezw) re_ezw,
                ROUND(SUM(CAST(B.target_io AS DOUBLE)), 0) AS target_io,
                ROUND(CASE WHEN CAST(B.target_io AS DOUBLE) = 0 OR SUM(A.io_ih) = 0 THEN 0 ELSE SUM(A.io_ih) / ROUND(SUM(CAST(B.target_io AS DOUBLE)), 0) * 100 END,1) AS drr_io,
                ROUND(SUM(CAST(B.target_re AS DOUBLE)), 0) AS target_re,
                ROUND(CASE WHEN CAST(B.target_re AS DOUBLE) = 0 OR SUM(A.re_ih) = 0 THEN 0 ELSE SUM(A.re_ih) / ROUND(SUM(CAST(B.target_re AS DOUBLE)), 0) * 100 END,1) AS drr_re,
                ROUND(SUM(CAST(B.target_ps AS DOUBLE)), 0) AS target_ps,
                ROUND(CASE WHEN CAST(B.target_ps AS DOUBLE) = 0 OR SUM(A.ps_ih) = 0 THEN 0 ELSE SUM(A.ps_ih) / ROUND(SUM(CAST(B.target_ps AS DOUBLE)), 0) * 100 END, 1) AS drr_ps
            FROM (
                SELECT branch, wok, sto,
                ps, io, re,
                ps_ih, io_ih, re_ih,
                ps_ezw, io_ezw, re_ezw
                FROM ranked A
                
                UNION ALL

                SELECT DISTINCT branch, wok, sto,
                0,0,0,0,0,0,0,0,0
                FROM puma_2025.ref_teritory_household A 
                WHERE regional = 'MALUKU DAN PAPUA' 
                AND NOT EXISTS (SELECT 1 rnk FROM ranked B WHERE A.sto = B.sto)
            ) A
            LEFT JOIN (SELECT DISTINCT branch, wok,sto FROM puma_2025.ref_teritory_household) X ON A.sto = X.sto
            LEFT JOIN household.target_io_ps_hh_v2 B ON X.sto = B.territory AND B.periode = ? AND B.flag_days = ?
            GROUP BY wok
            ORDER BY branch, wok
        `;

        const queryBranch = `
            WITH ranked AS (
                SELECT
                    branch,
                    wok,
                    sto_co as sto,
                    (COUNT(CASE WHEN DATE_FORMAT(ps_ts, '%Y-%m-%d') = ? THEN 1 END)) ps,
                    (COUNT(CASE WHEN DATE_FORMAT(io_ts, '%Y-%m-%d') = ? THEN 1 END)) io,
                    (COUNT(CASE WHEN DATE_FORMAT(re_ts, '%Y-%m-%d') = ? THEN 1 END)) re,
                    COUNT(CASE WHEN DATE_FORMAT(ps_ts, '%Y-%m-%d') = ? AND (product_commercial_name NOT LIKE '%Orbit%' AND product_commercial_name NOT LIKE '%Eznet%') THEN 1 END) ps_ih,
                    COUNT(CASE WHEN DATE_FORMAT(io_ts, '%Y-%m-%d') = ? AND (product_commercial_name NOT LIKE '%Orbit%' AND product_commercial_name NOT LIKE '%Eznet%') THEN 1 END) io_ih,
                    COUNT(CASE WHEN DATE_FORMAT(re_ts, '%Y-%m-%d') = ? AND (product_commercial_name NOT LIKE '%Orbit%' AND product_commercial_name NOT LIKE '%Eznet%') THEN 1 END) re_ih,
                    COUNT(CASE WHEN DATE_FORMAT(ps_ts, '%Y-%m-%d') = ? AND (product_commercial_name LIKE '%Orbit%' OR product_commercial_name LIKE '%Eznet%') THEN 1 END) ps_ezw,
                    COUNT(CASE WHEN DATE_FORMAT(io_ts, '%Y-%m-%d') = ? AND (product_commercial_name LIKE '%Orbit%' OR product_commercial_name LIKE '%Eznet%') THEN 1 END) io_ezw,
                    COUNT(CASE WHEN DATE_FORMAT(re_ts, '%Y-%m-%d') = ? AND (product_commercial_name LIKE '%Orbit%' OR product_commercial_name LIKE '%Eznet%') THEN 1 END) re_ezw,
                    RANK() OVER (ORDER BY COUNT(*) DESC) AS rnk
                FROM (select * from household.ih_ordering_detail_order_new) a
                WHERE region IN ('MALUKU DAN PAPUA', 'PUMA') AND order_type = 'NEW SALES'
                GROUP BY 1
            )
            SELECT
                A.branch,
                SUM(ps) ps, SUM(io) io, sum(re) re,
                sum(ps_ih) ps_ih, sum(io_ih) io_ih, SUM(re_ih) re_ih,
                sum(ps_ezw) ps_ezw, sum(io_ezw) io_ezw, SUM(re_ezw) re_ezw,
                ROUND(SUM(CAST(B.target_io AS DOUBLE)), 0) AS target_io,
                ROUND(CASE WHEN CAST(B.target_io AS DOUBLE) = 0 OR SUM(A.io_ih) = 0 THEN 0 ELSE SUM(A.io_ih) / ROUND(SUM(CAST(B.target_io AS DOUBLE)), 0) * 100 END,1) AS drr_io,
                ROUND(SUM(CAST(B.target_re AS DOUBLE)), 0) AS target_re,
                ROUND(CASE WHEN CAST(B.target_re AS DOUBLE) = 0 OR SUM(A.re_ih) = 0 THEN 0 ELSE SUM(A.re_ih) / ROUND(SUM(CAST(B.target_re AS DOUBLE)), 0) * 100 END,1) AS drr_re,
                ROUND(SUM(CAST(B.target_ps AS DOUBLE)), 0) AS target_ps,
                ROUND(CASE WHEN CAST(B.target_ps AS DOUBLE) = 0 OR SUM(A.ps_ih) = 0 THEN 0 ELSE SUM(A.ps_ih) / ROUND(SUM(CAST(B.target_ps AS DOUBLE)), 0) * 100 END, 1) AS drr_ps
            FROM (
                SELECT A.branch, A.wok, A.sto,
                A.ps, A.io, A.re,
                A.ps_ih, A.io_ih, A.re_ih,
                A.ps_ezw, A.io_ezw, A.re_ezw
                FROM ranked A

                UNION ALL
    
                SELECT DISTINCT branch, wok, sto,
                0,0,0,0,0,0,0,0,0
                FROM puma_2025.ref_teritory_household A
                WHERE regional = 'MALUKU DAN PAPUA' 
                    AND NOT EXISTS (SELECT 1 FROM ranked B WHERE A.sto = B.sto)
            ) A
            LEFT JOIN (SELECT DISTINCT branch, wok, sto FROM puma_2025.ref_teritory_household) X ON A.sto = X.sto
            LEFT JOIN household.target_io_ps_hh_v2 B ON X.sto = B.territory 
                AND B.periode = ? 
                AND B.flag_days = ?
            GROUP BY A.branch
            ORDER BY branch
        `;

        const queryIo = `
            SELECT sto_co,
            kabupaten,
            cluster,
            branch,
            wok,
            region,
            area,
            order_id,
            package_type,
            order_type,
            order_mode,
            order_initiator_id,
            order_initiator_id_type,
            process_state,
            provi_group,
            provi_subgroup,
            funneling_group,
            funneling_subgroup,
            DATE_FORMAT(provi_ts, '%Y-%m-%d %H:%i:%s') as provi_ts,
            provi_duration,
            name,
            no_handphone,
            address,
            segmentation,
            DATE_FORMAT(order_ts, '%Y-%m-%d %H:%i:%s') as order_ts,
            DATE_FORMAT(io_ts, '%Y-%m-%d %H:%i:%s') as io_ts,
            DATE_FORMAT(re_ts, '%Y-%m-%d %H:%i:%s') as re_ts,
            DATE_FORMAT(ps_ts, '%Y-%m-%d %H:%i:%s') as ps_ts,
            DATE_FORMAT(completed_ts, '%Y-%m-%d %H:%i:%s') as completed_ts,
            order_channel,
            channel_name,
            channel_group,
            service_id,
            product_id_co,
            product_commercial_name,
            package_cat,
            order_subtype,
            order_status,
            order_status_desc,
            payment_method,
            order_amount,
            tgl_manja,
            detail_manja,
            prev_state,
            DATE_FORMAT(prev_state_ts, '%Y-%m-%d %H:%i:%s') as prev_state_ts,
            longitude,
            latitude,
            appointment_id,
            DATE_FORMAT(appointment_start, '%Y-%m-%d %H:%i:%s') AS appointment_start,
            DATE_FORMAT(appointment_end, '%Y-%m-%d %H:%i:%s') AS appointment_end,
            reservation_id_odp,
            c_wonum,
            c_actstart,
            c_actfinish,
            c_errorcode,
            c_suberrorcode,
            c_engineermemo,
            c_urlevidence,
            c_amcrew,
            c_chief_code,
            c_chief_name,
            DATE_FORMAT(last_update, '%Y-%m-%d %H:%i:%s') as last_update,
            customer_account_id,
            channel_transaction_id,
            order_type_id,
            sf_name,
            sf_contact_number,
            sf_email,
            sf_company_name,
            sf_branch_name,
            DATE_FORMAT(ro_ts, '%Y-%m-%d %H:%i:%s') as ro_ts,
            DATE_FORMAT(pi_ts, '%Y-%m-%d %H:%i:%s') as pi_ts,
            DATE_FORMAT(pc_ts, '%Y-%m-%d %H:%i:%s') as pc_ts,
            odp_name,
            fallout_source,
            fallout_category,
            fallout_subcategory,
            fallout_reason,
            fiber_provisioning_type,
            product_id_orbit,
            cancel_category,
            cancel_reason,
            sla_duration,
            sf_code,
            subchannel,
            referral_code,
            product_type,
            deactivation_media,
            deactivation_reason,
            list_sn_ont,
            count_sn_ont,
            list_sn_stb,
            count_sn_stb,
            list_sn_orbit,
            list_msisdn_orbit,
            count_sn_orbit,
            order_id_prev,
            order_id_next,
            fee_psb,
            sa_id,
            DATE_FORMAT(fallout_ts, '%Y-%m-%d %H:%i:%s') as fallout_ts,
            prev_state_non,
            prev_state_non_funneling_subgroup,
            cancel_state,
            dmo_category,
            dmo_lp_name,
            product_name_co,
            product_id_desc,
            unit_type,
            unit_name,
            channel_subgroup,
            order_reason,
            order_reason_desc,
            order_id_demand,
            DATE_FORMAT(golive_ts, '%Y-%m-%d %H:%i:%s') as golive_ts,
            price_package,
            CAST(order_duration AS DOUBLE) as order_duration,
            region_nop,
            nop,
            product_id_before,
            product_commercial_name_before,
            speed_before,
            price_package_before,
            order_duration_cat,
            partner_id,
            sla_target_milestone,
            status_target_milestone,
            id_desa_adm,
            prov_adm,
            kab_adm,
            kec_adm,
            desa_adm,
            DATE_FORMAT(event_date, '%Y-%m-%d') as event_date
FROM household.ih_ordering_detail_order_new
            WHERE region IN ('MALUKU DAN PAPUA', 'PUMA') AND order_type = 'NEW SALES'
                AND DATE_FORMAT(io_ts, '%Y-%m-%d') = ?
            GROUP BY service_id
        `

        const dataIoToSheet = await queryToArray(queryIo, [today])

        const rowsWok = (await executeQuery(queryWok, [today, today, today, today, today, today, today, today, today, period, flagWeekend])) as any[]
        const rowsBranch = (await executeQuery(queryBranch, [today, today, today, today, today, today, today, today, today, period, flagWeekend])) as any[]

        if (!rowsWok || rowsWok.length === 0 || !rowsBranch || rowsBranch.length === 0) {
            await bot.sendMessage(chatId, `Household Hourly Sales Performance - ${endTime} WIB\n\nNo data found for the time range: ${startTime} to ${endTime}.`, { parse_mode: 'HTML' });
            return;
        }

        await clearSheet('IO!A:EC')
        await writeData('IO!A:EC', dataIoToSheet)

        const calculatedSalesBranch = rowsBranch.map(item => {
            const target_ps = Number(item.target_ps || 0);
            const target_io = Number(item.target_io || 0);
            const target_re = Number(item.target_re || 0);

            if (target_ps > 0 && target_io > 0) {
                return {
                    branch: item.branch,
                    ps: Number(item.ps || 0),
                    io: Number(item.io || 0),
                    re: Number(item.re || 0),
                    ps_ih: Number(item.ps_ih || 0),
                    io_ih: Number(item.io_ih || 0),
                    re_ih: Number(item.re_ih || 0),
                    ps_ezw: Number(item.ps_ezw || 0),
                    io_ezw: Number(item.io_ezw || 0),
                    re_ezw: Number(item.re_ezw || 0),
                    target_io: target_io,
                    drr_io: parseFloat(item.drr_io || 0),
                    target_re: target_re,
                    drr_re: parseFloat(item.drr_re || 0),
                    target_ps: target_ps,
                    drr_ps: parseFloat(item.drr_ps || 0)
                }
            }

            return {
                branch: item.branch,
                ps: Number(item.ps || 0),
                io: Number(item.io || 0),
                re: Number(item.re || 0),
                ps_ih: Number(item.ps_ih || 0),
                io_ih: Number(item.io_ih || 0),
                re_ih: Number(item.re_ih || 0),
                ps_ezw: Number(item.ps_ezw || 0),
                io_ezw: Number(item.io_ezw || 0),
                re_ezw: Number(item.re_ezw || 0),
                target_io: 0,
                drr_io: 0,
                target_re: 0,
                drr_re: 0,
                target_ps: 0,
                drr_ps: 0,
            }
        })

        const calculatedSalesWok = rowsWok.map(item => {
            const target_io = Number(item.target_io || 0);
            const target_ps = Number(item.target_ps || 0);
            const target_re = Number(item.target_re || 0);

            if (target_io > 0 && target_ps > 0) {
                return {
                    wok: item.wok,
                    ps: Number(item.ps || 0),
                    io: Number(item.io || 0),
                    re: Number(item.re || 0),
                    ps_ih: Number(item.ps_ih || 0),
                    io_ih: Number(item.io_ih || 0),
                    re_ih: Number(item.re_ih || 0),
                    ps_ezw: Number(item.ps_ezw || 0),
                    io_ezw: Number(item.io_ezw || 0),
                    re_ezw: Number(item.re_ezw || 0),
                    target_io: target_io,
                    drr_io: parseFloat(item.drr_io || 0),
                    target_re: target_re,
                    drr_re: parseFloat(item.drr_re || 0),
                    target_ps: target_ps,
                    drr_ps: parseFloat(item.drr_ps || 0)
                }
            }

            return {
                wok: item.wok,
                ps: Number(item.ps || 0),
                io: Number(item.io || 0),
                re: Number(item.re || 0),
                ps_ih: Number(item.ps_ih || 0),
                io_ih: Number(item.io_ih || 0),
                re_ih: Number(item.re_ih || 0),
                ps_ezw: Number(item.ps_ezw || 0),
                io_ezw: Number(item.io_ezw || 0),
                re_ezw: Number(item.re_ezw || 0),
                target_io: 0,
                drr_io: 0,
                target_re: 0,
                drr_re: 0,
                target_ps: 0,
                drr_ps: 0
            }
        })

        const totalPs = calculatedSalesBranch.reduce((sum, row) => sum + (row.ps || 0), 0)
        const totalIo = calculatedSalesBranch.reduce((sum, row) => sum + (row.io || 0), 0)
        const totalTargetIo = calculatedSalesBranch.reduce((sum, row) => sum + (row.target_io || 0), 0)
        const totalTargetPs = calculatedSalesBranch.reduce((sum, row) => sum + (row.target_ps || 0), 0)
        const drr_io = totalTargetIo != 0 || totalIo != 0 ? (totalIo / totalTargetIo * 100).toFixed(1) : '0'
        const drr_ps = totalTargetPs != 0 || totalPs != 0 ? (totalPs / totalTargetPs * 100).toFixed(1) : '0'

        const totalReBranch = calculatedSalesBranch.reduce((sum, row) => sum + (row.re || 0), 0);
        const totalRe = totalReBranch

        const totalReSalesIhBranch = calculatedSalesBranch.reduce((sum, row) => sum + (row.re_ih || 0), 0);
        const totalReSalesIh = totalReSalesIhBranch

        const totalReEzwBranch = calculatedSalesBranch.reduce((sum, row) => sum + (row.re_ezw || 0), 0);
        const totalReEzw = totalReEzwBranch

        const totalTargetRe = calculatedSalesBranch.reduce((sum, row) => sum + (row.target_re || 0), 0)

        const dataBranch: RowData[] = calculatedSalesBranch.map(item => ({
            territory: item.branch,
            io_ih6k: item.target_io.toString(),
            io_ezw: item.io_ezw.toString(),
            io_sales_ih: item.io_ih.toString(),
            io_ih6k_ach: item.drr_io + '%',
            io_total: item.io.toString(),
            re_ih6k: item.target_re.toString(),
            re_ezw: item.re_ezw.toString(),
            re_sales_ih: item.re_ih.toString(),
            re_total: item.re.toString(),
            re_ih6k_ach: calculatePercentage(item.re, item.target_re), // Fixed
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
            re_ih6k: item.target_re.toString(),
            re_ezw: item.re_ezw.toString(),
            re_sales_ih: item.re_ih.toString(),
            re_total: item.re.toString(),
            re_ih6k_ach: calculatePercentage(item.re, item.target_re), // Fixed
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
                io_ih6k: totalTargetIo.toString(),
                io_ezw: calculatedSalesBranch.reduce((sum, row) => sum + (row.io_ezw || 0), 0).toString(),
                io_sales_ih: calculatedSalesBranch.reduce((sum, row) => sum + (row.io_ih || 0), 0).toString(),
                io_total: (totalIo).toString(),
                io_ih6k_ach: calculatePercentage(calculatedSalesBranch.reduce((sum, row) => sum + (row.io_ih || 0), 0), totalTargetIo),
                re_ih6k: (totalTargetRe).toString(),
                re_ezw: totalReEzw.toString(),
                re_sales_ih: totalReSalesIh.toString(),
                re_total: totalRe.toString(),
                re_ih6k_ach: calculatePercentage(calculatedSalesBranch.reduce((sum, row) => sum + (row.re_ih || 0), 0), totalTargetRe),
                ps_ih6k: totalTargetPs.toString(),
                ps_ezw: calculatedSalesBranch.reduce((sum, row) => sum + (row.ps_ezw || 0), 0).toString(),
                ps_sales_ih: calculatedSalesBranch.reduce((sum, row) => sum + (row.ps_ih || 0), 0).toString(),
                ps_total: totalPs.toString(),
                ps_ach_ih6k: calculatePercentage(calculatedSalesBranch.reduce((sum, row) => sum + (row.ps_ih || 0), 0), totalTargetPs),
                ps_io_ps: calculatePercentage(totalPs, totalIo),
                ps_re_ps: calculatePercentage(totalPs, totalRe)
            },
            { territory: 'BRANCH', io_ih6k: '', io_ezw: '', io_sales_ih: '', io_total: '', io_ih6k_ach: '', re_ih6k: '', re_ezw: '', re_sales_ih: '', re_total: '', re_ih6k_ach: '', ps_ih6k: '', ps_ezw: '', ps_ach_ih6k: '', ps_io_ps: '', ps_re_ps: '', ps_total: '', ps_sales_ih: '' },
            ...dataBranch,
            { territory: 'WOK', io_ih6k: '', io_ezw: '', io_sales_ih: '', io_total: '', io_ih6k_ach: '', re_ih6k: '', re_ezw: '', re_sales_ih: '', re_total: '', re_ih6k_ach: '', ps_ih6k: '', ps_ezw: '', ps_ach_ih6k: '', ps_io_ps: '', ps_re_ps: '', ps_total: '', ps_sales_ih: '' },
            ...dataWok
        ]

        const imageBuffer = generateSalesTable(data, today, format(endTime, 'HH:mm:ss'))
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

        let message = ''

        if (currentTime.getHours() === 8 && currentTime.getMinutes() === 15) {
            const endTime = format(yesterday, 'yyyy-MM-dd')
            message = `
<pre><code>📊 Household Hourly Sales Performance
🗓 ${endTime} (Closing)\n
<b>═════ 🔥IO PERFORMANCE ═════</b>
<b>📌 PUMA Today : ${totalIo} IO (${totalTargetIo} | ${totalIo} | ${drr_io}%)</b>\n
<b>🏢 Branch RANK (Trg | Act | %Ach)</b>
${formattedIoBranch}\n
<b>🌍 WOK RANK (Trg | Act | %Ach)</b>
${formattedIoWok}\n
<b>═════ ⚡️PS PERFORMANCE ═════</b>
<b>📌 PUMA : ${totalPs} PS (${totalTargetPs} | ${totalPs} | ${drr_ps}%)</b>
<b>🔐 PUMA IOtoPS : ${ioToPs}%</b>\n
<b>🏢 Branch RANK (Trg | Act | %Ach)</b>
${formattedPsBranch}\n
<b>🌍 WOK RANK (Trg | Act | %Ach)</b>
${formattedPsWok}\n</code></pre>
`;
        } else {
            message = `
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
        }

        const ids = chatId.split(',')
        const sentTo: string[] = []

        for (const id of ids) {
            if (!id || sentTo.includes(id)) continue;

            try {
                const threadId = id.includes('/') ? id.split('/')[1] : undefined;
                if (threadId) {
                    const photoMsg = await bot.sendPhoto(id, imageBuffer, { parse_mode: 'HTML', message_thread_id: parseInt(threadId) }, { filename: 'temp_sales', contentType: 'image/png' })
                    await bot.sendMessage(id, message, { parse_mode: 'HTML', reply_to_message_id: photoMsg.message_id, message_thread_id: parseInt(threadId) })
                    sentTo.push(id)
                    return
                }
                const photoMsg = await bot.sendPhoto(id, imageBuffer, { parse_mode: 'HTML' }, { filename: 'temp_sales', contentType: 'image/png' })
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
cron.schedule('15 8,10-23 * * *', () => {
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