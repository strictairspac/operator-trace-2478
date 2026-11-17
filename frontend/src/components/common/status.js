import {traceState} from '../../constants/enums.js';
export const status=t=>{if(typeof t==='string')t={status:t};const s=traceState(t);return `<span class="status ${s.key}" title="${s.key}">${s.label}</span>`;};
