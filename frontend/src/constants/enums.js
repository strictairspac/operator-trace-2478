export const ROLES={admin:'管理员',operator:'操作员',viewer:'观察员'};
export const STATUSES={pending:'等待补送',queued:'已入队待领取',running:'执行中',completed:'已完成',failed:'执行失败'};
export const OPERATORS=['scan','filter','hash_join','group'];
export function traceState(t){
 if(t.status==='running')return t.recoveries>0?{key:'recovering',label:'接管重试中'}:{key:'running',label:'执行中'};
 if(t.status==='pending')return {key:'pending',label:t.recoveries>0?'等待重新接管':'等待补送'};
 if(t.status==='queued')return {key:'queued',label:t.recoveries>0?'已重新入队':'已入队待领取'};
 if(t.status==='completed')return {key:'completed',label:'已完成'};
 if(t.status==='failed')return {key:'failed',label:'执行失败'};
 return {key:t.status,label:t.status};
}
