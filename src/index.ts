import moment from "moment-timezone";
import { Database, Statement } from "sqlite3";
import sqlite3 from "sqlite3";
import fs from "fs";

function runQuery(dbase: Database, sql: string, params: Array<any>) {
    return new Promise<any>((resolve, reject) => {
        return dbase.all(sql, params, (err: any, res: any) => {
            if (err) {
                console.error("Run query Error: ", err.message);
                return reject(err.message);
            }
            return resolve(res);
        });
    });
}

function execStatement(stmt: Statement, params: any[]) {
    return new Promise<any>((resolve, reject) => {
        stmt.run(params, (err: any, res: any) => {
            if (err) {
                console.error("Run query Error: ", err.message);
                return reject(err.message);
            }
            return resolve(res);
        });
    });
}

async function generateMeasurements(year: number, timeZone: string) {

    let hourlyIterator = moment.tz([year, 0, 1], timeZone);
    let endOfYear = moment([year + 1, 0, 1]).tz(timeZone);
    let measuredValue = 0;
    let db: Database | null = null;
    let stmt: Statement | null = null;
    while (hourlyIterator.isBefore(endOfYear) || hourlyIterator.isSame(endOfYear)) {
        if ((hourlyIterator.get("date") == 1) && (hourlyIterator.get("hour") == 0)) {
            if (db && stmt) {
                stmt.finalize();
                await runQuery(db, "COMMIT", []);
            }
            let dbFileName = hourlyIterator.format("YYYY-MM") + '-monthly.sqlite';
            if (fs.existsSync(dbFileName)) {
                fs.rmSync(dbFileName)
            }
            db = new Database(dbFileName, sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE);
            console.log(moment().format(), `DB file '${dbFileName}' created.`);
            await runQuery(db, `CREATE TABLE "Measurements" ("id" INTEGER NOT NULL,"channel" INTEGER,"measured_value" REAL,"recorded_time" INTEGER, PRIMARY KEY("id" AUTOINCREMENT))`, []);
            await runQuery(db, "BEGIN", []);
            stmt = db.prepare("INSERT INTO Measurements (channel, measured_value, recorded_time) VALUES (?, ?, ?)");
        }
        if (db && stmt) {
            for (let channel = 1; channel <= 12; channel++) {
                try {
                    await execStatement(stmt, [channel, measuredValue, hourlyIterator.unix()]);
                } catch (err) {
                    console.error(err);
                }
                //await runQuery(db, `INSERT INTO Measurements (channel, measured_value, recorded_time) VALUES (?, ?, ?)`, [channel, measuredValue, hourlyIterator.unix()]);
            }
        }
        measuredValue += 100;
        hourlyIterator.add(1, "hour");
    }
    if (db && stmt) {
        stmt.finalize();
        await runQuery(db, "COMMIT", []);
    }
}

generateMeasurements(2022, "America/Los_Angeles");