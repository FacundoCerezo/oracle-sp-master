import { createLogger, format, transports } from "winston";
const { combine, timestamp, label, printf } = format;

const customFormat = combine(
    label({ label: "Logging" }),
    timestamp(),
    printf((info) => {
        return `${info.timestamp} ${info.level}:${info.message}`;
    })
);
let logger = createLogger({
    format: customFormat,
    transports: [
        new transports.Console(),
    ]
});
export { logger };
