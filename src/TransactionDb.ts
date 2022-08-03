import { StoreProcedureDb } from './StoreProcedureDb'
import { logger } from "./CustomLogger";
import { plainToClass } from 'class-transformer';
import { SpResult } from './SpResult';
// tslint:disable-next-line: no-var-requires
const oracledb = require('oracledb');

export class TransactionDb {
    public storeProceduresList: StoreProcedureDb[];
    public constructor(storeProceduresList: StoreProcedureDb[]) {
        this.storeProceduresList = storeProceduresList;
    }
    public async executeTransaction() {
        let connection;
        if (this.storeProceduresList && this.storeProceduresList.length) {
            try {
                connection = await oracledb.getConnection();
                logger.info('Comienzo de transacción, cantidad de SP a ejecutar: ' + this.storeProceduresList.length)
                for (const sp of this.storeProceduresList) {
                    await sp.executeTransactionalSp(connection).then(async (x: SpResult[]) => {
                        const result: SpResult[] = plainToClass(SpResult, x, {
                            excludeExtraneousValues: true,
                        });
                        if (result && result[0] && result[0].mensaje != 'OK') {
                            throw new Error(result[0].error);
                        }
                    });
                    logger.info('Finalizo la ejecución del SP')
                }
                logger.info('Finalizo la ejecución de la transacción.')
                await connection.commit();
                logger.info('Commiteo la transacción.')
            } catch (e) {
                logger.error('Finalizo la ejecución de la transacción con error.', e)
            } finally {
                if (connection) {
                    try {
                        await connection.close();
                    } catch (err) {
                        logger.error(err);
                    }
                }
            }
        }
    }

    public async executeTransactionWithDependency() {
        let connection;
        if (this.storeProceduresList && this.storeProceduresList.length) {
            try {
                connection = await oracledb.getConnection();
                logger.info('Comienzo de transacción, cantidad de SP a ejecutar: ' + this.storeProceduresList.length);
                let dependentId;
                for (const sp of this.storeProceduresList) {
                    if (sp.isPreviousDependent) {
                        sp.parameters.push(dependentId);
                    }
                    await sp.executeTransactionalSp(connection).then(async (x: SpResult[]) => {
                        const result: SpResult[] = plainToClass(SpResult, x, {
                            excludeExtraneousValues: true,
                        });
                        if (result && result[0] && result[0].mensaje != 'OK') {
                            throw new Error(result[0].error);
                        }
                        if (result[0] && result[0].id) {
                            logger.info('Se devolvió un Id que va a servir para el siguiente SP ' + result[0].id);
                            dependentId = result[0].id;
                        }
                    });
                    logger.info('Finalizo la ejecución del SP');
                }
                logger.info('Finalizo la ejecución de la transacción.');
                await connection.commit();
                logger.info('Commiteo la transacción.')
            } catch (e) {
                logger.error('Finalizo la ejecución de la transacción con error.', e);
            } finally {
                if (connection) {
                    try {
                        await connection.close();
                    } catch (err) {
                        logger.error(err);
                    }
                }
            }
        }
    }
}