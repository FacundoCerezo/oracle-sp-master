import { logger } from './CustomLogger';

const oracledb = require('oracledb');
oracledb.fetchAsBuffer = [oracledb.BLOB];
oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;

// tslint:disable-next-line: no-var-requires
const numRows = 100000;

export class StoreProcedureDb {
    public name: string;
    public execution: string;
    public parameters: any[];
    public size: number;
    public autoCommit: boolean;
    public isPreviousDependent: boolean;
    public isLast: boolean;
    private isCursorAtLast: boolean;

    public constructor(name: string, parameters?: any[], autoCommit?: boolean, isPreviousDependent?: boolean,
                       isLast?: boolean) {
        this.name = name;
        this.autoCommit = autoCommit;
        this.isPreviousDependent = isPreviousDependent;
        this.isLast = isLast;
        if (parameters && parameters.length) {
            for (let i = 0; i <= parameters.length - 1; i++) {
                if (typeof parameters[i] === 'string') {
                    parameters[i] = parameters[i];
                }
            }
        }
        this.parameters = parameters;
    }

    public cursorAtLast(): StoreProcedureDb {
        this.isCursorAtLast = true;
        return this;
    }

    public async executeSp(): Promise<any> {
        let connection;
        try {
            connection = await oracledb.getConnection();
            // ver https://github.com/oracle/node-oracledb/issues/1194#issuecomment-579279555
            const resultSet = await this.setResultSet(connection);
            let rows = new Array();
            let row;
            while (row = await resultSet.getRow()){
                rows.push(row);
            }
            await resultSet.close();

/*             const resultSet = await this.setResultSet(connection);
            let rows;
            rows = await resultSet.getRows(numRows);
            await resultSet.close(); */

            if (this.autoCommit) {
                await connection.commit();
            }
            return rows;
        } catch (err) {
            logger.error(this.execution, err);
            return undefined;
        } finally {
            if (connection) {
                try {
                    await connection.close();
                    // await connection.release();
                    logger.info(`Cerro la sesión correctamente.. ${this.execution}`);
                } catch (err) {
                    logger.error(`Intentando cerrar la conexión falló... ${this.execution}`, err);
                }
            }
        }
    }

    public async executeBlobSp(): Promise<any> {
        let connection;
        try {
            connection = await oracledb.getConnection();
            let stringParams: string = '';
            const valueParams: { [k: string]: any } = {};
            const size = this.parameters ? this.parameters.length || 0 : 0;
            if (this.parameters) {
                for (let i = 0; i < size; i++) {
                    if (i === size - 1) {
                        stringParams += `:${i}`;
                        valueParams[i] = {val: this.parameters[i]};
                    } else {
                        stringParams += `:${i},`;
                        valueParams[i] = {val: this.parameters[i]};
                    }
                }
            }
            valueParams['blob'] = {
                type: oracledb.BUFFER,
                dir: oracledb.BIND_OUT,
                maxSize: 500000
            };
            const execution = this.parameters
                              && this.parameters.length ? `${this.name}(:blob,${stringParams});` : `${this.name}(:blob);`;
            logger.info(this.parameters && this.parameters.length ? `${execution} Params values: ${JSON.stringify(
                this.parameters)}` : `${execution}`);
            const result = await connection.execute(
                `BEGIN
               ${execution}
               END;`,
                valueParams
            );
            const buf = result.outBinds.blob;
            logger.info('Primer buffer log');
            logger.info(JSON.stringify(buf));
            logger.info('Segundo buffer log');
            if (this.autoCommit) {
                await connection.commit();
            }
            return buf;
        } catch (err) {
            logger.error(err);
            return undefined;
        } finally {
            if (connection) {
                try {
                    await connection.close();
                    // await connection.release();
                    logger.info(`Cerro la sesión correctamente.. ${this.execution}`);
                } catch (err) {
                    logger.error('Intentando cerrar la conexión falló..', this.execution, err);
                }
            }
        }
    }

    public async executeTransactionalSp(connection: any): Promise<any> {
        try {
            const resultSet = await this.setResultSet(connection);
            let rows = new Array();
            let row;
            while (row = await resultSet.getRow()){
                rows.push(row);
            }
            await resultSet.close();
            return rows;
        } catch (err) {
            logger.error(this.execution, err);
            return undefined;
        }
    }

    private getExecutionWithCursor(stringParams: string): string {
        let execution: string = `${this.name}(:cursor);`;
        if (this.parameters && this.parameters.length) {
            execution = `${this.name}(:cursor,${stringParams});`;
            if (!!this.isCursorAtLast)
                execution = `${this.name}(${stringParams},:cursor);`
        }
        this.execution = this.parameters && this.parameters.length ? `${execution} Params values: ${JSON.stringify(
            this.parameters)}` : `${execution}`;
        logger.info(this.execution);
        return execution;
    }

    private async setResultSet(connection: any): Promise<any> {
        let stringParams: string = '';
        const valueParams: { [k: string]: any } = {};
        const size = this.parameters ? this.parameters.length || 0 : 0;
        if (this.parameters) {
            for (let i = 0; i < size; i++) {
                if (i === size - 1) {
                    stringParams += `:${i}`;
                    valueParams[i] = {val: this.parameters[i]};
                } else {
                    stringParams += `:${i},`;
                    valueParams[i] = {val: this.parameters[i]};
                }
            }
        }
        valueParams['cursor'] = {
            type: oracledb.CURSOR,
            dir: oracledb.BIND_OUT
        };
        const execution = this.getExecutionWithCursor(stringParams);
        const result = await connection.execute(
            `BEGIN
               ${execution}
               END;`,
            valueParams
        );
        const resultSet = result.outBinds.cursor;
        return resultSet;
    }
}
