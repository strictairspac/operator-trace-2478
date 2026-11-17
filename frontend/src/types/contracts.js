/** @typedef {{id:number,name:string,sensors:object[],readings:object[]}} Dataset */
/** @typedef {{id:number,name:string,dataset_id:number,steps:object[]}} Plan */
/** @typedef {{id:number,name:string,status:'pending'|'queued'|'running'|'completed'|'failed',result:object|null,attempt:number,recoveries:number,error:string|null}} Trace */
export {};
