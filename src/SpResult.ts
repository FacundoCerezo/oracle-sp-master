import { Expose } from 'class-transformer';

export class SpResult {
    @Expose({name: 'Id'})
    id: number;
    @Expose({name: 'Mensaje'})
    mensaje: string;
    @Expose({name: 'Error'})
    error: string;

    constructor(id?: number, mensaje?: string, error?: string) {
        this.id = id ? id : null;
        this.mensaje = mensaje ? mensaje : null;
        this.error = error ? error : null;
    }
}
