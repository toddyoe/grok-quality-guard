package main

func dashboardHTML() string {
	return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Grok Quality Guard</title>
<style>
:root{color-scheme:dark;--bg:#0d1117;--panel:#161b22;--line:#30363d;--text:#e6edf3;--muted:#8b949e;--accent:#58a6ff;--good:#3fb950;--warn:#d29922;--bad:#f85149}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}main{max-width:1280px;margin:0 auto;padding:24px}h1{font-size:24px;margin:0 0 4px}h2{font-size:16px;margin:24px 0 12px}.muted{color:var(--muted)}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}.card,.panel{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:14px}.card strong{display:block;font-size:24px;margin-top:4px}.panel{margin-top:16px;overflow:auto}table{width:100%;border-collapse:collapse;min-width:980px}th,td{text-align:left;padding:9px 8px;border-bottom:1px solid var(--line);vertical-align:top}th{color:var(--muted);font-weight:600}button{background:#21262d;color:var(--text);border:1px solid var(--line);border-radius:6px;padding:5px 8px;cursor:pointer;margin:2px}button:hover{border-color:var(--accent)}button.primary{background:#1f6feb;border-color:#1f6feb}button.danger{border-color:#8b2c2c;color:#ffb3ad}textarea{width:100%;min-height:260px;background:#0d1117;color:var(--text);border:1px solid var(--line);border-radius:8px;padding:10px;font:12px/1.5 ui-monospace,monospace}.row{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.badge{padding:2px 7px;border-radius:999px;background:#21262d;border:1px solid var(--line);font-size:12px}.state-healthy{color:var(--good)}.state-suspect{color:var(--warn)}.state-degraded,.state-cooling{color:var(--bad)}.state-disabled_by_guard,.state-externally_disabled{color:var(--muted)}pre{white-space:pre-wrap;word-break:break-word;background:#0d1117;border:1px solid var(--line);border-radius:8px;padding:10px;max-height:260px;overflow:auto}#msg{min-height:20px;margin:8px 0;color:var(--warn)}input{background:#0d1117;color:var(--text);border:1px solid var(--line);border-radius:6px;padding:7px 9px;min-width:0}.form-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px}.field{display:flex;flex-direction:column;gap:5px}.field label{color:var(--muted);font-size:12px}.field input[type=text],.field input[type=url],.field input[type=password]{width:100%}.check{display:flex;gap:8px;align-items:center;margin:8px 0}.check input{width:auto}.field-note{color:var(--muted);font-size:12px}.resin-status{margin-top:10px;color:var(--good)}
</style>
</head>
<body><main>
<h1>Grok Quality Guard</h1>
<div class="muted">CPA plugin dashboard. Credentials and proxy secrets are never displayed.</div>
<div id="msg"></div>
<div class="cards" id="cards"></div>

<h2>Accounts</h2>
<div class="panel"><table><thead><tr><th>Account</th><th>State</th><th>Reason</th><th>Resin</th><th>Metrics</th><th>Cooldown / probe</th><th>Actions</th></tr></thead><tbody id="accounts"></tbody></table></div>

<h2>Resin</h2>
<div class="panel">
<div class="muted">Admin API and proxy credentials are validated before they are saved. Secret values are never returned to this page.</div>
<div class="form-grid" style="margin-top:12px">
<div class="field"><label for="resin_platform">Platform name</label><input id="resin_platform" type="text" autocomplete="off"></div>
<div class="field"><label for="resin_platform_id">Platform ID (UUID)</label><input id="resin_platform_id" type="text" autocomplete="off"></div>
<div class="field"><label for="resin_admin_url">Admin API URL</label><input id="resin_admin_url" type="url" autocomplete="off" placeholder="http://resin:2260"></div>
<div class="field"><label for="resin_admin_token">Admin Token</label><input id="resin_admin_token" type="password" autocomplete="new-password"><span id="resin_admin_token_hint" class="field-note"></span></div>
<div class="field"><label for="resin_proxy_token">Proxy Token</label><input id="resin_proxy_token" type="password" autocomplete="new-password"><span id="resin_proxy_token_hint" class="field-note"></span></div>
<div class="field"><label for="resin_proxy_template">Proxy template</label><input id="resin_proxy_template" type="text" autocomplete="off"></div>
</div>
<div class="check"><input id="resin_enabled" type="checkbox"><label for="resin_enabled">Enable Resin binding and rotation</label></div>
<div class="check"><input id="resin_auto_rotate" type="checkbox"><label for="resin_auto_rotate">Rotate after confirmed quality failure</label></div>
<div class="row"><button class="primary" onclick="saveResin()">Validate and save</button><button onclick="validateResin()">Validate only</button><span id="resin_status" class="resin-status"></span></div>
</div>

<h2>Policy</h2>
<div class="panel"><textarea id="policy"></textarea><div class="row"><button class="primary" onclick="savePolicy()">Save policy</button><button onclick="resetPolicy()">Reset to YAML defaults</button><button onclick="bindUnbound()">Bind unbound Resin</button><button onclick="refresh()">Refresh</button></div></div>

<h2>Events</h2>
<div class="panel"><table><thead><tr><th>Time</th><th>Auth</th><th>Class</th><th>Action</th><th>Result</th><th>Detail</th></tr></thead><tbody id="events"></tbody></table></div>
</main>
<script>
const API='/v0/management/plugins/grok-quality-guard/api';
const esc=(v)=>String(v??'').replace(/[&<>"']/g,(c)=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const stateClass=(s)=>'state-'+(s||'unknown');
async function api(action,params={}){const r=await fetch(API,{method:'POST',headers:{'Content-Type':'application/json','X-Grok-Quality-Guard-UI':'1'},body:JSON.stringify({action,params})});const j=await r.json();if(!j.ok)throw new Error((j.error&&j.error.message)||'request failed');return j.data;}
function msg(t){document.getElementById('msg').textContent=t||''}
function metricText(a){return 'TPS '+Number(a.tps||0).toFixed(1)+' · TTFT '+a.ttft_ms+'ms · out '+a.output_tokens+' · reason '+a.reasoning_tokens+' · enc '+a.encrypted_bytes}
function renderOverview(o){const states=o.states||{};const cards=[['Grok auths',o.auth_count],['Healthy',states.healthy||0],['Suspect',states.suspect||0],['Degraded',states.degraded||0],['Cooling',states.cooling||0],['Disabled',states.disabled_by_guard||0],['Resin bound',(o.resin||{}).bound||0],['24h anomalies',o.recent_anomalies_24h||0]];document.getElementById('cards').innerHTML=cards.map(([k,v])=>'<div class="card"><span class="muted">'+esc(k)+'</span><strong>'+esc(v)+'</strong></div>').join('')}
function renderAccounts(items){document.getElementById('accounts').innerHTML=(items||[]).map(a=>{const auth=esc(a.auth_id);return '<tr><td><div>'+esc(a.display)+'</div><div class="muted">'+esc(a.auth_id_display||a.auth_id)+'</div></td><td><span class="badge '+esc(stateClass(a.state))+'">'+esc(a.state)+'</span></td><td>'+esc(a.last_reason||'')+'</td><td>'+esc(a.proxy_status)+(a.resin_account?'<div class="muted">'+esc(a.resin_account)+' #'+esc(a.resin_generation)+'</div>':'')+'</td><td class="muted">'+esc(metricText(a))+'</td><td class="muted">'+esc(a.cooldown_until||'')+'<br>'+esc(a.probe_backoff_until||'')+'</td><td><button data-auth="'+auth+'" data-op="accounts.probe" onclick="act(this.dataset.op,{auth_id:this.dataset.auth,force:true})">Probe</button><button data-auth="'+auth+'" data-op="accounts.cooldown" onclick="act(this.dataset.op,{auth_id:this.dataset.auth,duration:\'12h\'})">Cool</button><button data-auth="'+auth+'" data-op="accounts.restore" onclick="act(this.dataset.op,{auth_id:this.dataset.auth})">Restore</button><button class="danger" data-auth="'+auth+'" data-op="accounts.disable" onclick="act(this.dataset.op,{auth_id:this.dataset.auth,reason:\'manual\'})">Disable</button><button data-auth="'+auth+'" data-op="resin.bind" onclick="act(this.dataset.op,{auth_id:this.dataset.auth})">Bind</button><button data-auth="'+auth+'" data-op="resin.rotate" onclick="act(this.dataset.op,{auth_id:this.dataset.auth})">Rotate</button></td></tr>'}).join('')}
function renderEvents(items){document.getElementById('events').innerHTML=(items||[]).map(e=>'<tr><td>'+esc(e.time)+'</td><td>'+esc(e.auth_id||'')+'</td><td>'+esc(e.class||'')+'</td><td>'+esc(e.action||'')+'</td><td>'+esc(e.result||'')+'</td><td>'+esc(e.detail||'')+'</td></tr>').join('')}
function renderResinConfig(c){document.getElementById('resin_platform').value=c.platform||'';document.getElementById('resin_platform_id').value=c.platform_id||'';document.getElementById('resin_admin_url').value=c.admin_url||'';document.getElementById('resin_proxy_template').value=c.proxy_template||'';document.getElementById('resin_enabled').checked=!!c.enabled;document.getElementById('resin_auto_rotate').checked=!!c.auto_rotate_after_quality_failure;document.getElementById('resin_admin_token').value='';document.getElementById('resin_proxy_token').value='';document.getElementById('resin_admin_token_hint').textContent=c.admin_token_set?'Configured; leave blank to keep':'Required';document.getElementById('resin_proxy_token_hint').textContent=c.proxy_token_set?'Configured; leave blank to keep':'Required';document.getElementById('resin_status').textContent=c.configured&&c.validated_at?'Validated '+c.validated_at:'Not configured'}
function resinInput(){return {enabled:document.getElementById('resin_enabled').checked,platform:document.getElementById('resin_platform').value,platform_id:document.getElementById('resin_platform_id').value,admin_url:document.getElementById('resin_admin_url').value,proxy_template:document.getElementById('resin_proxy_template').value,auto_rotate_after_quality_failure:document.getElementById('resin_auto_rotate').checked,admin_token:document.getElementById('resin_admin_token').value,proxy_token:document.getElementById('resin_proxy_token').value}}
async function validateResin(){try{document.getElementById('resin_status').textContent='Validating…';const r=await api('resin.config.validate',resinInput());document.getElementById('resin_status').textContent='Admin API OK · admin token OK · platform OK · proxy token OK · '+r.validated_at}catch(err){document.getElementById('resin_status').textContent='Validation failed: '+err.message}}
async function saveResin(){try{document.getElementById('resin_status').textContent='Validating…';await api('resin.config.save',resinInput());document.getElementById('resin_status').textContent='Saved after successful validation';msg('Resin configuration saved');await refresh()}catch(err){document.getElementById('resin_status').textContent='Save rejected: '+err.message}}
async function refresh(){try{msg('Loading…');const [o,a,e,p,r]=await Promise.all([api('overview'),api('accounts.list',{page_size:200}),api('events.list',{limit:100}),api('policy.get'),api('resin.config.get')]);renderOverview(o);renderAccounts(a.items);renderEvents(e.items);document.getElementById('policy').value=JSON.stringify(p,null,2);renderResinConfig(r);msg('')}catch(err){msg(err.message)}}
async function act(action,params){try{msg('Running '+action+'…');await api(action,params);msg('Done');await refresh()}catch(err){msg(err.message)}}
async function savePolicy(){try{const p=JSON.parse(document.getElementById('policy').value);await api('policy.update',{policy:p});msg('Policy saved');await refresh()}catch(err){msg(err.message)}}
async function resetPolicy(){try{await api('policy.reset');msg('Policy reset');await refresh()}catch(err){msg(err.message)}}
async function bindUnbound(){try{msg('Binding unbound accounts…');const r=await api('resin.bind_unbound',{dry_run:false});msg('Processed '+r.count+' account(s)');await refresh()}catch(err){msg(err.message)}}
refresh();
</script>
</body></html>`
}
