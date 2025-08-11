import { ComplexEventEmitter } from 'ts-utils/event-emitter';

export type Events = {
    error: [Error];
};

export default new ComplexEventEmitter<Events>();
