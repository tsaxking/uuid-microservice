import { Struct } from "drizzle-struct/back-end";
import { createStructEventService } from "../services/struct-event";
import test from './test-schema';

Struct.each(createStructEventService);
test('false');