const amqp = require('amqplib');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { spawn } = require('child_process');

// --- تنظیمات ---
// این مقادیر رو متناسب با تنظیمات خودت پر کن
const CONFIG = {
    // شناسه این کلاینت. باید با ClientId که از سرور میاد یکی باشه
    // توی کد C# سرور، این مقدار از printerData.PlaceID میاد که یک int هست
    CLIENT_ID: 1, // مثلا: 1 یا 2 (مقدار "branch01" اینجا اشتباهه)

    RABBIT_HOST: 'amqp://guest:guest@5.238.111.18', // آدرس RabbitMQ
    QUEUE_NAME: 'print'                            // اسم صف
};
// --- پایان تنظیمات ---

const SCRIPT_PATH = path.join(__dirname, 'print-image.ps1');

async function startListener() {
    console.log('Connecting to RabbitMQ...');
    try {
        const connection = await amqp.connect(CONFIG.RABBIT_HOST);
        const channel = await connection.createChannel();
        
        await channel.assertQueue(CONFIG.QUEUE_NAME, { durable: true });
        
        console.log(`Waiting for messages in queue: ${CONFIG.QUEUE_NAME}`);
        console.log(`This client ID is: ${CONFIG.CLIENT_ID}`);

        channel.consume(CONFIG.QUEUE_NAME, (msg) => {
            if (msg !== null) {
                processMessage(msg, channel);
            }
        }, { noAck: false }); // noAck: false یعنی ما دستی تایید (ack) می‌کنیم

    } catch (err) {
        console.error('Connection error', err.message);
        // ۵ ثانیه بعد دوباره تلاش کن
        setTimeout(startListener, 5000);
    }
}

async function processMessage(msg, channel) {
    let job;
    let tempFilePath = null;

    try {
        const messageBody = msg.content.toString();
        job = JSON.parse(messageBody);

        // چک کردن ClientId
        // مطمئن شو که نوع داده‌ها یکی هست (هر دو عدد هستن)
        if (job.ClientId !== CONFIG.CLIENT_ID) {
            console.log(`Ignoring job ${job.JobId} for client ${job.ClientId}`);
            // این پیام برای ما نیست. Nack می‌زنیم و میگیم requeue=true
            // تا یک کلاینت دیگه اون رو برداره
            channel.nack(msg, false, true);
            return;
        }

        console.log(`Processing job ${job.JobId} for printer ${job.PrinterName}`);

        // ۱. ذخیره فایل موقت
        const imageBuffer = Buffer.from(job.Base64Image, 'base64');
        const tempFileName = `print_${job.JobId || Date.now()}.png`;
        tempFilePath = path.join(os.tmpdir(), tempFileName);
        
        fs.writeFileSync(tempFilePath, imageBuffer);

        // ۲. فراخوانی اسکریپت PowerShell
        await printWithPowerShell(tempFilePath, job.PrinterName);

        // ۳. تایید پیام (Ack)
        console.log(`Job ${job.JobId} completed successfully.`);
        channel.ack(msg); // حذف پیام از صف

    } catch (err) {
        console.error(`Failed to process job ${job ? job.JobId : 'UNKNOWN'}:`, err.message);
        // پیام رو Nack می‌کنیم ولی requeue=true که دوباره تلاش بشه
        // (شاید پرینتر خاموش بوده)
        channel.nack(msg, false, true); 
    } finally {
        // ۴. پاک کردن فایل موقت
        if (tempFilePath && fs.existsSync(tempFilePath)) {
            try {
                fs.unlinkSync(tempFilePath);
            } catch (e) {
                console.error('Failed to delete temp file', tempFilePath);
            }
        }
    }
}

function printWithPowerShell(filePath, printerName) {
    return new Promise((resolve, reject) => {
        const args = [
            '-NoProfile',
            '-NonInteractive',
            '-ExecutionPolicy', 'Bypass',
            '-File', SCRIPT_PATH,
            '-FilePath', filePath,
            '-PrinterName', printerName
        ];

        const ps = spawn('powershell', args, { windowsHide: true });

        let stdout = '', stderr = '';
        ps.stdout.on('data', d => stdout += d.toString());
        ps.stderr.on('data', d => stderr += d.toString());

        ps.on('close', code => {
            if (code === 0) {
                resolve(stdout.trim());
            } else {
                reject(new Error(stderr || stdout || `PowerShell exited with code ${code}`));
            }
        });

        ps.on('error', err => {
            reject(err);
        });
    });
}

// اسکریپت پاورشل رو چک کن
if (!fs.existsSync(SCRIPT_PATH)) {
    console.error(`Error: PowerShell script not found at ${SCRIPT_PATH}`);
    console.error('Please make sure "print-image.ps1" is in the same directory.');
    process.exit(1);
}

startListener();
