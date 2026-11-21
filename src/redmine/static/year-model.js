(() => {
  "use strict";
  const $ = id => document.getElementById(id);
  const esc = value => String(value ?? "").replace(/[&<>"']/g, ch => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[ch]));
  const fmt = value => new Intl.NumberFormat("ru-RU", {maximumFractionDigits:1}).format(Number(value) || 0);
  const editAmount = value => String(value ?? "0").replace(".", ",");
  const teamCosts = team => team.monthly_costs || Array(12).fill(team.monthly_cost ?? "0");
  const confidences = {none:"Нет",low:"Низкая",medium:"Средняя",high:"Высокая",fact:"Факт"};
  const kinds = {direction:"Направление",team:"Команда",topic:"Тема",decision:"Решение"};
  const months = ["Январь","Февраль","Март","Апрель","Май","Июнь","Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь"];
  const shortMonths = ["янв","фев","мар","апр","май","июн","июл","авг","сен","окт","ноя","дек"];
  let model = null, editing = null, controller = null, currentQuery = "", busy = false;
  let collapsedDirections = new Set(), preferencesReady = false, savedYear = null;
  let preferenceQueue = Promise.resolve(), preferenceRequest = 0;
  function saveView(patch) {
    if (!preferencesReady) return;
    const request = ++preferenceRequest;
    $("viewStatus").textContent = "Сохраняем вид…";
    preferenceQueue = preferenceQueue.then(async () => {
      try {
        await api("/api/year-model/view-settings", {
          method:"PUT", headers:{"Content-Type":"application/json"}, body:JSON.stringify(patch), keepalive:true,
        });
        if (patch.selected_year !== undefined) savedYear = patch.selected_year;
        if (request === preferenceRequest) $("viewStatus").textContent = "";
      } catch(error) {
        $("viewStatus").textContent = `Не удалось сохранить вид: ${error.message}`;
      }
    });
  }
  const isHistorical = () => model && model.as_of < model.today;
  const selectedConfidence = () => [...document.querySelectorAll(".confidence-filter input:checked")].map(input => input.value);
  const dayLabel = item => {
    const [, month, day] = item.expected_date.split("-").map(Number);
    return item.date_precision === "month" ? shortMonths[month-1] : `${day} ${shortMonths[month-1]}`;
  };
  const timestamp = value => new Intl.DateTimeFormat("ru-RU", {dateStyle:"short",timeStyle:"medium",timeZone:"Europe/Samara"}).format(new Date(value));
  async function api(url, options = {}) {
    const response = await fetch(url, {credentials:"same-origin",cache:"no-store",...options});
    if (!response.ok) {
      let detail;
      try { detail = (await response.json()).detail; } catch { detail = `Ошибка ${response.status}`; }
      if (response.status === 401) detail = "Сессия завершена. Войдите в приложение и обновите страницу.";
      throw new Error(typeof detail === "string" ? detail : "Проверьте заполнение полей");
    }
    return response;
  }
  function queryFromControls() {
    const params = new URLSearchParams();
    if ($("year").value) params.set("year", $("year").value);
    if ($("asOf").value) params.set("as_of", $("asOf").value);
    params.set("confidence", selectedConfidence().join(","));
    return params.toString();
  }
  async function load() {
    if (!$('year').checkValidity() || !$('asOf').checkValidity()) {
      $('year').reportValidity(); $('asOf').reportValidity(); return;
    }
    controller?.abort();
    const active = new AbortController(); controller = active;
    const query = queryFromControls();
    $("status").className = ""; $("status").textContent = "Загружаем модель…";
    $("board").classList.add("loading"); $("apply").disabled = true;
    try {
      const result = await (await api(`/api/year-model?${query}`, {signal:active.signal})).json();
      model = result; currentQuery = query;
      $("year").value = model.year; $("asOf").value = model.as_of;
      $("years").innerHTML = model.years.map(year => `<option value="${year}"></option>`).join("");
      const url = new URL(location.href); url.search = queryFromControls(); history.replaceState(null,"",url);
      render(); $("status").textContent = "";
      if (model.year !== savedYear) saveView({selected_year:model.year});
    } catch (error) {
      if (error.name !== "AbortError") { $("status").className = "error"; $("status").textContent = `Не удалось загрузить модель. ${error.message}`; }
    } finally {
      if (controller === active) { $("board").classList.remove("loading"); $("apply").disabled = false; }
    }
  }
  function summaryTable(summary) {
    const rows = [["target","Цель по выручке"],["costs","Затраты"],["revenue","Выручка"],["revenue_total","Выручка (нар. итог)"]];
    rows.push(["balance","Баланс"]);
    return `<table class="summary-table"><tbody>${rows.map(([key,label]) => `<tr class="${key}"><td>${label}</td>${summary[key].map((value,index) => `<td class="${value < 0 ? 'negative' : ''}">${index > 0 && (key === 'target' || key === 'costs' && value === 0) ? '' : fmt(value)}</td>`).join("")}</tr>`).join("")}</tbody></table>`;
  }
  function eventLayout(events, year) {
    const rows = [], placements = [];
    const width = $("monthHeaders").getBoundingClientRect().width || 720;
    const span = Math.min(.5, Math.max(2/12, 170/width));
    for (const event of events) {
      const [,month,day] = event.expected_date.split("-").map(Number);
      const days = new Date(year,month,0).getDate();
      const anchor = (month - 1 + (event.date_precision === "month" ? .08 : (day - 1) / days)) / 12;
      const start = Math.min(anchor, 1-span), end = start + span;
      let row = rows.findIndex(value => value <= start);
      if (row < 0) row = rows.length;
      rows[row] = end;
      placements.push({event,start,anchor,row,span});
    }
    return {placements,height:Math.max(228,rows.length*92+20)};
  }
  function lane(direction) {
    const layout = eventLayout(direction.events, model.year);
    const title = direction.company ? "Сводно по всем направлениям" : `${direction.teams.length} команд · ${direction.events.length} событий`;
    const disabled = isHistorical() ? "disabled" : "";
    const teamLabels = direction.teams.map(team => `<button class="team-name ${team.attention ? 'needs-attention' : ''}" data-edit="${esc(team.id)}" title="${esc(team.name)} · ${fmt(team.annual_cost)} млн ₽/год${team.attention ? ' · Обратить внимание' : ''}"><span>${esc(team.name)}</span></button>`).join("");
    const costs = direction.teams.map(team => `<div class="team-costs">${team.monthly_costs.map((value,index) => `<button type="button" class="team-cost" data-team-cost="${esc(team.id)}" data-month="${index+1}" aria-label="Затраты: ${esc(team.name)}, ${months[index]} ${model.year}" title="${esc(team.name)} · ${months[index]} · ${fmt(value)} млн ₽">${fmt(value)}</button>`).join("")}</div>`).join("");
    const teamTotal = (value,name) => `<div class="team-total-row" title="${esc(name)} · затраты за 12 месяцев, млн ₽"><span>${fmt(value)}</span></div>`;
    const teamTotals = direction.teams.map(team => teamTotal(team.annual_cost,team.name)).join("");
    const monthActions = isHistorical() ? '' : `<div class="month-create-grid">${months.map((month,index) => `<button type="button" data-create-topic="${direction.id}" data-month="${index+1}" aria-label="Создать тему: ${esc(direction.name)}, ${month} ${model.year}" title="Двойной щелчок — создать тему · ${month} ${model.year}"></button>`).join('')}</div>`;
    const cards = layout.placements.map(({event,start,anchor,row,span}) => {
      const style = `--left:${start*100}%;--anchor:${anchor*100}%;--top:${20+row*92}px;--width:${span*100}%`;
      return `<span class="event-marker ${event.confidence}" style="${style}" aria-hidden="true"></span><span class="event-stem ${event.confidence}" style="${style}" aria-hidden="true"></span><button class="event ${event.confidence} ${event.kind} ${event.attention ? 'needs-attention' : ''}" style="${style}" data-edit="${esc(event.id)}" title="${esc(event.name)} · ${confidences[event.confidence]} · ${dayLabel(event)} · ${fmt(event.amount)} млн ₽${event.attention ? ' · Обратить внимание' : ''}"><span class="event-title">${event.kind === 'decision' ? '◆ ' : ''}${esc(event.name)}</span><span class="event-foot"><span class="event-date">${dayLabel(event)} · ${confidences[event.confidence]}</span><span class="event-amount">${fmt(event.amount)}</span></span></button>`;
    }).join("");
    const collapsed = collapsedDirections.has(direction.id);
    const miniMonths = months.map((month,index) => {
      const events = direction.events.filter(event => Number(event.expected_date.slice(5,7)) === index+1);
      return `<span class="mini-month">${events.length ? `<span class="month-count" title="${esc(month+': '+events.map(event=>event.name).join('; '))}" aria-label="${esc(month)}: событий ${events.length}">${events.length}</span>` : ''}</span>`;
    }).join("");
    return `<section class="lane ${direction.company ? 'company' : ''} ${collapsed ? 'is-collapsed' : ''}" data-lane="${direction.id}" aria-label="${esc(direction.name)}" style="--event-height:${layout.height}px">
      <div class="lane-label"><div class="direction-label"><div class="direction-heading">
        <button class="lane-toggle" data-toggle="${direction.id}" aria-expanded="${!collapsed}" aria-controls="timeline-${direction.id}" aria-label="${collapsed ? 'Развернуть' : 'Свернуть'} направление ${esc(direction.name)}"><span aria-hidden="true">${collapsed ? '+' : '−'}</span></button>
        <button class="direction-name ${direction.attention ? 'needs-attention' : ''}" data-edit="${esc(direction.id)}" title="${esc(direction.name)}${direction.attention ? ' · Обратить внимание' : ''}">${esc(direction.name)}</button></div>
        <div class="direction-meta">${title}</div><div class="lane-actions"><button data-add="topic" data-direction="${direction.id}" ${disabled}>+ Тема</button><button data-add="decision" data-direction="${direction.id}" ${disabled}>+ Решение</button>${direction.company ? '' : `<button data-add="team" data-direction="${direction.id}" ${disabled}>+ Команда</button>`}</div></div>
        ${teamLabels}${direction.company ? '<div class="team-name company-total">Затраты команд / месяц</div>' : ''}</div>
      <div class="timeline"><div class="lane-detail" id="timeline-${direction.id}" ${collapsed ? 'hidden' : ''}><div class="events">${monthActions}${cards}</div>${costs}${direction.company ? `<div class="team-costs">${direction.monthly_costs.map(value=>`<span class="team-cost company-total">${fmt(value)}</span>`).join('')}</div>` : ''}</div><div class="mini-months" ${collapsed ? '' : 'hidden'}>${miniMonths}</div></div>
      <div class="lane-summary"><div class="direction-summary">${summaryTable(direction.summary)}</div>${teamTotals}${direction.company ? teamTotal(direction.team_annual_cost,'Все команды') : ''}</div></section>`;
  }
  function renderLanes() {
    if (!model) return;
    $("lanes").innerHTML = model.directions.length ? model.directions.map(lane).join("") : '<div class="empty-board">Добавьте направление, чтобы начать планирование. Направление «Компания» появится автоматически.</div>';
  }

  function render() {
    $("modelTitle").textContent = `План на ${model.year} год`;
    $("snapshotLabel").textContent = `Состояние на ${model.as_of.split('-').reverse().join('.')}`;
    const company = model.directions.find(direction=>direction.company);
    $("overview").hidden = !company;
    if (company) {
      const s = company.summary;
      const metrics = [["Цель по выручке",s.target[0],"Все направления"],["Затраты",s.costs.reduce((a,b)=>a+b,0),"Команды и выбранные решения"],["Выручка",s.revenue[0],"Высокая + факт"],["Потенциал",s.revenue[1]+s.revenue[2],"Средняя + низкая"],["Баланс компании",s.balance[0],`Со средними: ${fmt(s.balance[1])} · с низкими: ${fmt(s.balance[2])}`]];
      $("overview").innerHTML = metrics.map(([label,value,note])=>`<div class="metric"><span class="metric-label">${label}</span><span class="metric-value">${fmt(value)}</span><small>${note}</small></div>`).join("");
    }
    const messages = [];
    if (isHistorical()) messages.push("Исторический срез: доступен просмотр и история. Для изменений выберите сегодняшнюю дату.");
    if (!model.directions.length) messages.push(`На выбранную дату данных за ${model.year} год нет. Предварительное заполнение находится в 2027 году.${model.year !== 2027 ? ' <button id="openSeedYear" class="button">Открыть 2027</button>' : ''}`);
    $("notice").hidden = !messages.length; $("notice").innerHTML = messages.join("<br>");
    $("openSeedYear")?.addEventListener("click",()=>{$("year").value=2027;$("asOf").value=model.today;load();});
    $("addDirection").disabled = isHistorical();
    $("importLabel").hidden = !!model.directions.length || isHistorical();
    renderLanes();
  }
  function field(name,label,value="",options={}) {
    const attrs = `${options.required ? 'required' : ''} ${options.readonly ? 'readonly' : ''}`;
    let input;
    if (options.choices) input = `<select name="${name}" ${attrs}>${options.choices.map(([key,text])=>`<option value="${esc(key)}" ${String(value) === String(key) ? 'selected' : ''}>${esc(text)}</option>`).join("")}</select>`;
    else if (options.textarea) input = `<textarea name="${name}" maxlength="10000">${esc(value)}</textarea>`;
    else input = `<input name="${name}" type="${options.type || 'text'}" value="${esc(value)}" ${attrs} ${options.extra || ''}>`;
    return `<label class="${options.full ? 'full' : ''}">${label}${input}${options.note ? `<small>${options.note}</small>` : ''}</label>`;
  }
  const checkbox = (name,label,checked=false) => `<label class="checkbox-field full"><input type="checkbox" name="${name}" ${checked ? 'checked' : ''}>${label}</label>`;
  function openEditor(kind, item=null, directionId="", defaults={}, costMonth=null) {
    if (!model) return;
    editing = {kind,item,costMonth}; $("editError").textContent=""; $("history").hidden=true; $("history").innerHTML="";
    const d = item || defaults, historical = isHistorical();
    $("editorTitle").textContent = `${item ? '' : 'Новое: '}${kinds[kind]}${historical ? ' · просмотр' : ''}`;
    let fields = field("name","Название",d.name,{required:true,full:true,readonly:d.company,extra:'maxlength="250"'});
    if (kind === "direction") {
      fields += field("target",d.company ? "Собственная цель компании, млн ₽" : "Целевая выручка, млн ₽", editAmount(d.target), {required:true,full:true,extra:'inputmode="decimal"',note:d.company ? "При консолидации к ней добавляются цели всех направлений." : ""});
    } else {
      const directions = model.directions.filter(dir=>kind!=="team" || !dir.company).map(dir=>[dir.id,dir.name]);
      fields += field("direction_id","Направление",d.direction_id || directionId,{required:true,full:true,choices:directions});
      if (kind === "team") {
        if (item) fields += '<p class="field-note full">Для изменения затрат щелкните сумму в нужном месяце на ленте.</p>';
        else {
          fields += field("cost_month","Начиная с месяца",1,{choices:months.map((month,index)=>[index+1,month])});
          fields += field("monthly_cost","Затраты, млн ₽","0",{required:true,extra:'inputmode="decimal"',note:"Отрицательная сумма. До выбранного месяца затраты равны нулю."});
          fields += checkbox("copy_forward","Скопировать на новые месяцы",true);
        }
      }
      else {
        fields += field("amount","Стоимость, млн ₽",editAmount(d.amount),{required:true,extra:'inputmode="decimal"',note:kind === "decision" ? "Затраты — со знаком минус." : ""});
        fields += field("confidence","Уверенность",d.confidence || "high",{required:true,choices:Object.entries(confidences)});
        fields += field("expected_date","Ожидаемая дата",d.expected_date || `${model.year}-01-01`,{required:true,type:"date",extra:`min="${model.year}-01-01" max="${model.year}-12-31"`});
        fields += field("date_precision","Точность даты",d.date_precision || "month",{choices:[["day","Точный день"],["month","Месяц"]]});
        if (kind === "topic") {
          fields += field("url","Ссылка на источник (CRM)",d.url,{type:"url",full:true,extra:'maxlength="2000" placeholder="https://…"'});
          if (d.url) fields += `<a class="source-link full" href="${esc(d.url)}" target="_blank" rel="noopener noreferrer">Открыть источник в CRM</a>`;
        }
      }
    }
    fields += checkbox("attention","Обратить внимание",d.attention === true);
    fields += field("note",kind === "direction" || kind === "team" ? "Комментарий" : "Примечание",d.note,{textarea:true,full:true});
    if (costMonth !== null) {
      $("editorTitle").textContent = `Затраты команды · ${months[costMonth-1]}${historical ? ' · просмотр' : ''}`;
      fields = `<p class="full cost-team-title">${esc(d.name)} · ${model.year}</p>`;
      fields += field("monthly_cost","Затраты, млн ₽",editAmount(teamCosts(d)[costMonth-1]),{required:true,full:true,extra:'inputmode="decimal"',note:"Отрицательная сумма. Снятая галочка изменит только выбранный месяц."});
      fields += checkbox("copy_forward","Скопировать на новые месяцы",true);
      fields += '<p class="field-note full">При включенной галочке сумма применяется от выбранного месяца до декабря включительно.</p>';
    }
    if (item) fields += `<p class="version-note">Версия ${item.revision} · ${timestamp(item.recorded_at)} (Самара)<br>${esc(item.actor)}</p>`;
    $("editFields").innerHTML=fields;
    $("deleteItem").hidden=!item || d.company || historical || costMonth !== null;
    $("historyButton").hidden=!item;
    $("saveItem").hidden=historical;
    $("editFields").querySelectorAll("input,select,textarea").forEach(el=>el.disabled=historical);
    $("editor").showModal();
  }
  async function save(deleted=false) {
    if (busy || !editing) return;
    const {kind,item,costMonth} = editing;
    if (deleted && !confirm(`Удалить «${item.name}»? Предыдущие версии сохранятся в исторических срезах.`)) return;
    busy=true;$("saveItem").disabled=true;$("deleteItem").disabled=true;$("editError").textContent="";
    const data = Object.fromEntries(new FormData($("editForm")));
    if (costMonth === null) data.attention=$("editForm").elements.attention.checked;
    if (kind === "team" && (!item || costMonth !== null)) data.copy_forward=$("editForm").elements.copy_forward.checked;
    if (costMonth !== null) Object.assign(data,{name:item.name,direction_id:item.direction_id,note:item.note,cost_month:costMonth});
    try {
      await api(`/api/year-model/items${item ? '/'+encodeURIComponent(item.id) : ''}`,{
        method:deleted ? "DELETE" : item ? "PUT" : "POST",headers:{"Content-Type":"application/json"},
        body:JSON.stringify({year:model.year,kind,data,revision:item?.revision ?? null}),
      });
      $("editor").close(); await load();
    } catch(error) { $("editError").textContent=error.message; }
    finally {busy=false;$("saveItem").disabled=false;$("deleteItem").disabled=false;}
  }
  async function showHistory() {
    if (!editing?.item) return;
    const entityId=editing.item.id;
    $("history").hidden=false;$("history").textContent="Загружаем историю…";
    const labels={name:"Название",direction_id:"Направление",target:"Цель",monthly_cost:"Затраты / месяц",monthly_costs:"Затраты по месяцам",amount:"Стоимость",confidence:"Уверенность",expected_date:"Ожидаемая дата",date_precision:"Точность даты",attention:"Обратить внимание",url:"CRM",note:editing.kind === "direction" || editing.kind === "team" ? "Комментарий" : "Примечание",deleted:"Удалено"};
    try {
      const data=await (await api(`/api/year-model/items/${encodeURIComponent(entityId)}/history?as_of=${model.as_of}`)).json();
      if (editing?.item?.id !== entityId) return;
      $("history").innerHTML=`<h3>История изменений · время Самары</h3>${data.versions.map(version=>`<article><time>${timestamp(version.recorded_at)}</time> · версия ${version.revision}<br>${esc(version.actor)}<dl>${Object.entries(labels).filter(([key])=>version[key]!==undefined && version[key]!=="" && !(key==="monthly_cost" && version.monthly_costs)).map(([key,label])=>{
        let value=version[key];
        if (["target","monthly_cost","amount"].includes(key)) value=fmt(value)+" млн ₽";
        if (key==="confidence") value=confidences[value];
        if (key==="direction_id") value=model.items.find(item=>item.id===value)?.name || "Направление недоступно в этом срезе";
        if (key==="date_precision") value=value==="month" ? "Месяц" : "Точный день";
        if (key==="deleted") value="Да";
        if (key==="attention") value=value ? "Да" : "Нет";
        if (key==="monthly_costs") value=value.map((amount,index)=>`${months[index]}: ${fmt(amount)}`).join('; ')+" млн ₽";
        return `<dt>${label}</dt><dd>${esc(value)}</dd>`;
      }).join("")}</dl></article>`).join("")}`;
    } catch(error) {$("history").textContent=error.message;}
  }
  $("board").addEventListener("click",event=>{
    const toggle=event.target.closest("[data-toggle]");
    if (toggle) {
      const id=toggle.dataset.toggle;
      const collapsed=!collapsedDirections.has(id);
      collapsed ? collapsedDirections.add(id) : collapsedDirections.delete(id);
      const direction=model.directions.find(item=>item.id===id);
      toggle.closest(".lane").outerHTML=lane(direction);
      $("lanes").querySelector(`[data-toggle="${id}"]`).focus({preventScroll:true});
      saveView({direction_id:id,collapsed});
      return;
    }
    const edit=event.target.closest("[data-edit]");
    if (edit) {const item=model.items.find(item=>item.id===edit.dataset.edit);if(item)openEditor(item.kind,item);return;}
    const add=event.target.closest("[data-add]");
    if (add) {openEditor(add.dataset.add,null,add.dataset.direction);return;}
    const cost=event.target.closest("[data-team-cost]");
    if (cost) {
      const team=model.items.find(item=>item.id===cost.dataset.teamCost);
      if (team) openEditor("team",team,"",{},Number(cost.dataset.month));
    }
  });
  function createTopicInMonth(event) {
    const month=event.target.closest("[data-create-topic]");
    if (month && !isHistorical()) openEditor("topic",null,month.dataset.createTopic,{
      expected_date:`${model.year}-${month.dataset.month.padStart(2,'0')}-01`,date_precision:"month",
    });
  }
  $("board").addEventListener("dblclick",createTopicInMonth);
  $("board").addEventListener("keydown",event=>{
    if ((event.key === "Enter" || event.key === " ") && event.target.matches("[data-create-topic]")) {
      event.preventDefault(); createTopicInMonth(event);
    }
  });
  $("apply").addEventListener("click",load);
  $("importFile").addEventListener("change",async event=>{
    const file=event.target.files[0]; if(!file)return;
    $("status").textContent="Загружаем начальный план…";
    try {
      if(file.size>2000000)throw new Error("Файл плана не должен превышать 2 МБ");
      const payload=JSON.parse(await file.text());
      if(payload.year !== model.year)throw new Error(`Сначала выберите год ${payload.year}, указанный в плане`);
      await api("/api/year-model/import",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)});
      await load();
    } catch(error){$("status").className="error";$("status").textContent=`Не удалось загрузить план: ${error.message}`;}
    finally{event.target.value="";}
  });
  $("addDirection").addEventListener("click",()=>openEditor("direction"));
  $("editForm").addEventListener("submit",event=>{event.preventDefault();save();});
  $("editFields").addEventListener("input",event=>{
    const input=event.target;
    if (!input.matches('input[inputmode="decimal"]') || !input.value.includes('.')) return;
    const start=input.selectionStart, end=input.selectionEnd;
    input.value=input.value.replaceAll('.',',');
    input.setSelectionRange(start,end);
  });
  $("deleteItem").addEventListener("click",()=>save(true));
  $("historyButton").addEventListener("click",showHistory);
  ["closeEditor","cancelEdit"].forEach(id=>$(id).addEventListener("click",()=>$("editor").close()));
  $("export").addEventListener("click",async()=>{
    if(!model)return;
    $("export").disabled=true;
    try {
      const response=await api(`/api/year-model/export?${currentQuery}`);
      const url=URL.createObjectURL(await response.blob());const a=document.createElement("a");
      a.href=url;a.download=`Модель-${model.year}.xlsx`;a.click();setTimeout(()=>URL.revokeObjectURL(url),10000);
    } catch(error){$("status").className="error";$("status").textContent=error.message;}
    finally{$("export").disabled=false;}
  });
  $("monthHeaders").innerHTML = months.map((month,index)=>`<div class="month ${index % 3 === 0 ? 'quarter' : ''}" title="${month}"><span class="month-full">${month}</span><span class="month-short">${shortMonths[index][0].toUpperCase()+shortMonths[index].slice(1)}</span></div>`).join("");
  let lastTimelineWidth = 0, resizeFrame = null;
  new ResizeObserver(entries => {
    const width = entries[0].contentRect.width;
    if (Math.abs(width-lastTimelineWidth)<1) return;
    lastTimelineWidth = width;
    cancelAnimationFrame(resizeFrame);
    resizeFrame = requestAnimationFrame(renderLanes);
  }).observe($("monthHeaders"));
  async function initialize() {
    const initial=new URLSearchParams(location.search);
    try {
      const preferences=await (await api("/api/year-model/view-settings")).json();
      collapsedDirections=new Set(preferences.collapsed_directions);
      savedYear=preferences.selected_year;
      preferencesReady=true;
      if(savedYear)$("year").value=savedYear;
    } catch(error) {$("viewStatus").textContent=`Не удалось восстановить сохраненный вид: ${error.message}`;}
    if(initial.has("year"))$("year").value=initial.get("year");
    if(initial.has("as_of"))$("asOf").value=initial.get("as_of");
    if(initial.has("confidence"))document.querySelectorAll(".confidence-filter input").forEach(input=>input.checked=initial.get("confidence").split(",").includes(input.value));
    await load();
  }
  initialize();
})();
