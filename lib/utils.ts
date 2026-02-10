import { createCanvas } from "canvas";

interface RowData {
    territory: string;
    io_ih6k: string;
    io_ezw: string;
    io_sales_ih: string;
    io_total: string;
    io_ih6k_ach: string;
    re_ih6k: string;
    re_ezw: string;
    re_sales_ih: string;
    re_total: string;
    re_ih6k_ach: string;
    ps_ih6k: string;
    ps_ezw: string;
    ps_sales_ih: string;
    ps_total: string;
    ps_ach_ih6k: string;
    ps_io_ps: string;
    ps_re_ps: string;
}

function generateSalesTable(data: RowData[], trx_date: string, last_update: string) {
    const width = 1600;
    const height = 750;
    const canvas = createCanvas(width, height);
    const ctx = canvas.getContext('2d');

    // Background
    ctx.fillStyle = '#ffffff';
    ctx.fillRect(0, 0, width, height);

    // Title section with better typography hierarchy
    ctx.fillStyle = '#1a1a1a';
    ctx.font = 'bold 26px Arial';
    ctx.fillText('Household Hourly Sales Performance', 20, 40);

    ctx.fillStyle = '#555555';
    ctx.font = '16px Arial';
    ctx.fillText('Region Level Group by Sales Type', 20, 65);

    ctx.fillStyle = '#666666';
    ctx.font = '14px Arial';
    ctx.fillText(`Transaction Date: ${trx_date}`, 20, 85);
    ctx.fillText(`Last Update: ${last_update}`, 20, 105);

    // Table settings
    const colWidth = 80;
    const rowHeight = 32;
    const startX = 20;
    const startY = 120;
    const regionWidth = 200;

    // Consistent border styling
    const borderColor = '#e0e0e0';
    const borderWidth = 1;
    const headerBorderColor = '#cccccc';

    // Helper function to draw borders
    function drawCellBorder(x: number, y: number, width: number, height: number, isHeader = false) {
        ctx.strokeStyle = isHeader ? headerBorderColor : borderColor;
        ctx.lineWidth = borderWidth;
        ctx.strokeRect(x, y, width, height);
    }

    // Helper function to draw centered text
    function drawCenteredText(text: string, x: number, y: number, width: number, height: number) {
        const textX = x + width / 2;
        const textY = y + height / 2 + 4; // +4 for better vertical centering
        ctx.textAlign = 'center';
        ctx.fillText(text, textX, textY);
    }

    // Helper function to draw left-aligned text (for region names)
    function drawLeftText(text: string, x: number, y: number, height: number, padding = 8) {
        const textY = y + height / 2 + 4;
        ctx.textAlign = 'left';
        ctx.fillText(text, x + padding, textY);
    }

    // Region header (spans 3 rows)
    ctx.fillStyle = '#2c3e50';
    ctx.fillRect(startX, startY, regionWidth, rowHeight * 3);
    drawCellBorder(startX, startY, regionWidth, rowHeight * 3, true);

    ctx.fillStyle = '#ffffff';
    ctx.font = 'bold 16px Arial';
    drawCenteredText('REGION', startX, startY, regionWidth, rowHeight * 3);

    // Level 1 Headers (IO, RE, PS) - Main categories
    let x = startX + regionWidth;
    ctx.font = 'bold 15px Arial';

    const headerStructure = [
        { name: 'IO', columns: 5, color: '#3498db' },
        { name: 'RE', columns: 5, color: '#e74c3c' },
        { name: 'PS', columns: 7, color: '#27ae60' }
    ];

    for (const header of headerStructure) {
        ctx.fillStyle = header.color;
        ctx.fillRect(x, startY, colWidth * header.columns, rowHeight);
        drawCellBorder(x, startY, colWidth * header.columns, rowHeight, true);

        ctx.fillStyle = '#ffffff';
        drawCenteredText(header.name, x, startY, colWidth * header.columns, rowHeight);
        x += colWidth * header.columns;
    }

    // Level 2 Headers (Target, Sales, Achievement, etc.)
    x = startX + regionWidth;
    ctx.font = 'bold 14px Arial';

    const subHeaderStructure = [
        // IO section
        { name: 'Target', columns: 1, color: '#bdc3c7' },
        { name: 'Sales', columns: 3, color: '#bdc3c7' },
        { name: '%Ach', columns: 1, color: '#bdc3c7' },
        // RE section
        { name: 'Target', columns: 1, color: '#bdc3c7' },
        { name: 'Sales', columns: 3, color: '#bdc3c7' },
        { name: '%Ach', columns: 1, color: '#bdc3c7' },
        // PS section
        { name: 'Target', columns: 1, color: '#bdc3c7' },
        { name: 'Sales', columns: 3, color: '#bdc3c7' },
        { name: '%Ach', columns: 3, color: '#bdc3c7' }
    ];

    for (const subHeader of subHeaderStructure) {
        ctx.fillStyle = subHeader.color;
        ctx.fillRect(x, startY + rowHeight, colWidth * subHeader.columns, rowHeight);
        drawCellBorder(x, startY + rowHeight, colWidth * subHeader.columns, rowHeight, true);

        ctx.fillStyle = '#2c3e50';
        drawCenteredText(subHeader.name, x, startY + rowHeight, colWidth * subHeader.columns, rowHeight);
        x += colWidth * subHeader.columns;
    }

    // Level 3 Headers (IH 6K, EZW, etc.)
    const thirdLevelHeaders = [
        'IH 7K', 'EZW', 'IH', 'Total', 'IH 7K', // IO
        'IH 7K', 'EZW', 'IH', 'Total', 'IH 7K', // RE
        'IH 7K', 'EZW', 'IH', 'Total', 'IH 7K', 'IO/PS', 'RE/PS' // PS
    ];

    x = startX + regionWidth;
    ctx.font = 'bold 12px Arial';

    for (const header of thirdLevelHeaders) {
        ctx.fillStyle = '#f8f9fa';
        ctx.fillRect(x, startY + rowHeight * 2, colWidth, rowHeight);
        drawCellBorder(x, startY + rowHeight * 2, colWidth, rowHeight, true);

        ctx.fillStyle = '#495057';
        drawCenteredText(header, x, startY + rowHeight * 2, colWidth, rowHeight);
        x += colWidth;
    }

    // Data keys mapping
    const dataKeys: (keyof RowData)[] = [
        'io_ih6k', 'io_ezw', 'io_sales_ih', 'io_total', 'io_ih6k_ach',
        're_ih6k', 're_ezw', 're_sales_ih', 're_total', 're_ih6k_ach',
        'ps_ih6k', 'ps_ezw', 'ps_sales_ih', 'ps_total', 'ps_ach_ih6k',
        'ps_io_ps', 'ps_re_ps'
    ];

    // Draw data rows
    let y = startY + rowHeight * 3;
    ctx.font = '13px Arial';

    for (const row of data) {
        x = startX;

        // Draw region cell with improved styling
        let regionBgColor = '#ffffff';
        let regionTextColor = '#2c3e50';

        if (row.territory === 'NATIONAL') {
            regionBgColor = '#e74c3c';
            regionTextColor = '#ffffff';
        } else if (row.territory.includes('BRANCH') || row.territory.includes('WOK')) {
            regionBgColor = '#ecf0f1';
        }

        ctx.fillStyle = regionBgColor;
        ctx.fillRect(x, y, regionWidth, rowHeight);
        drawCellBorder(x, y, regionWidth, rowHeight);

        ctx.fillStyle = regionTextColor;
        ctx.font = row.territory === 'NATIONAL' ? 'bold 12px Arial' : '12px Arial';
        drawLeftText(row.territory, x, y, rowHeight);

        x += regionWidth;

        // Draw data cells with better formatting
        ctx.font = '13px Arial';
        for (const key of dataKeys) {
            const value = row[key];
            let cellBgColor = '#ffffff';
            let textColor = '#2c3e50';

            // Color coding for percentage values
            if (value.endsWith('%')) {
                const val = parseFloat(value.replace('%', ''));
                if (!isNaN(val)) {
                    if (val < 50) {
                        cellBgColor = '#ffebee';
                        textColor = '#c62828';
                    } else if (val >= 50 && val < 100) {
                        cellBgColor = '#fff3e0';
                        textColor = '#ef6c00';
                    } else if (val >= 100) {
                        cellBgColor = '#e8f5e8';
                        textColor = '#2e7d32';
                    }
                }
            }

            if (!value.endsWith('%') && !isNaN(Number(value))) {
                const val = parseFloat(value)
                if (val === 0) {
                    cellBgColor = '#ffebee';
                    textColor = '#c62828';
                }
            }

            ctx.fillStyle = cellBgColor;
            ctx.fillRect(x, y, colWidth, rowHeight);
            drawCellBorder(x, y, colWidth, rowHeight);

            ctx.fillStyle = textColor;
            drawCenteredText(value, x, y, colWidth, rowHeight);

            x += colWidth;
        }
        y += rowHeight;
    }

    // Footer with better styling
    ctx.fillStyle = '#7f8c8d';
    ctx.font = '10px Arial';
    ctx.textAlign = 'left';
    ctx.fillText('Source: Hadoop', 20, height - 40);
    ctx.fillText('* All Products (IndiHome Regular, EZnet Wireless Rent, TBG, RT RW)', 20, height - 20);

    return canvas.toBuffer('image/png');
}

const calculatePercentage = (numerator: number, denominator: number): string => {
    if (denominator === 0) {
        return '0%';
    }
    const result = (numerator / denominator) * 100;
    // Ensure result is a valid number before formatting
    if (Number.isFinite(result)) {
        return result.toFixed(1) + '%';
    }
    return '0%';
};

export { generateSalesTable, calculatePercentage, type RowData };