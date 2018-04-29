/* tslint:disable:no-empty */
import {ILogger} from "./index";
import debug from "debug";

export const NOOP = () => {};
/* tslint:enable */

export const NOOPL: ILogger = {
  debug: debug("sinek:debug"),
  error: debug("sinek:error"),
  info: debug("sinek:info"),
  warn: debug("sinek:warn"),
};
