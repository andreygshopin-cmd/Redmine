from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlencode

from src.redmine.bitrix_client import (
    BITRIX_DEAL_ENTITY_TYPE_ID,
    BITRIX_LEAD_ENTITY_TYPE_ID,
    BITRIX_PAGE_SIZE,
    callBitrixRestMethod,
    fetchBitrixCrmItemFields,
    fetchBitrixDealDictionaries,
    fetchBitrixUserNames,
)


COMMON_ANALYSIS_FIELDS = [
    "id",
    "title",
    "stageId",
    "statusId",
    "stageSemanticId",
    "assignedById",
    "opportunity",
    "currencyId",
    "companyId",
    "createdTime",
    "updatedTime",
    "movedTime",
    "lastActivityTime",
    "sourceId",
    "closed",
]
DEAL_ANALYSIS_FIELDS = [
    *COMMON_ANALYSIS_FIELDS,
    "categoryId",
    "begindate",
    "closedate",
    "contactId",
    "leadId",
]
LEAD_ANALYSIS_FIELDS = [
    *COMMON_ANALYSIS_FIELDS,
    "companyTitle",
    "hasPhone",
    "hasEmail",
]
DEAL_CONTROL_EXCLUDED_STAGES = {"договор в работе", "тендер/закупка"}


def _chunks(values: list[int], size: int = 50) -> list[list[int]]:
    return [values[index:index + size] for index in range(0, len(values), size)]


def _normalizeId(value: object) -> int | None:
    try:
        normalized = int(value or 0)
    except (TypeError, ValueError):
        return None
    return normalized if normalized > 0 else None


def _asBool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().casefold() in {"1", "true", "y", "yes"}


def _asFloat(value: object) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _parseDateTime(value: object) -> datetime | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        return datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None


def _dateYear(value: object) -> int | None:
    parsed = _parseDateTime(value)
    return parsed.year if parsed else None


def _daysSince(value: object, asOfDate: date) -> int | None:
    parsed = _parseDateTime(value)
    if not parsed:
        return None
    return max(0, (asOfDate - parsed.date()).days)


def _activityAge(item: dict[str, object], asOfDate: date) -> int:
    activityAge = _daysSince(_activityTime(item), asOfDate)
    if activityAge is not None:
        return activityAge
    createdAge = _daysSince(item.get("createdTime"), asOfDate)
    return createdAge if createdAge is not None else 0


def _activityTime(item: dict[str, object]) -> object:
    return item.get("_analysisLastActivityTime") or item.get("lastActivityTime")


def _dateTimeSortKey(value: datetime) -> float:
    normalized = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return normalized.timestamp()


def _latestDateTimeValue(*values: object) -> str | None:
    candidates = [
        (parsed, str(value))
        for value in values
        if (parsed := _parseDateTime(value)) is not None
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda candidate: _dateTimeSortKey(candidate[0]))[1]


def _applyLatestCompletedActivityTimes(
    items: list[dict[str, object]],
    latestCompletedTimes: dict[int, str],
) -> None:
    for item in items:
        itemId = _normalizeId(item.get("id"))
        latestTime = _latestDateTimeValue(
            item.get("lastActivityTime"),
            latestCompletedTimes.get(itemId) if itemId else None,
        )
        if latestTime:
            item["_analysisLastActivityTime"] = latestTime


def _semantic(item: dict[str, object]) -> str:
    return str(item.get("stageSemanticId") or "").strip().upper()


def _isOpen(item: dict[str, object]) -> bool:
    semantic = _semantic(item)
    if semantic:
        return semantic == "P"
    return not _asBool(item.get("closed"))


def _stageId(item: dict[str, object]) -> str:
    return str(item.get("statusId") or item.get("stageId") or "").strip()


def _selectAvailableFields(
    portalUrl: str,
    credential: str,
    entityTypeId: int,
    requestedFields: list[str],
) -> list[str]:
    availableFields = fetchBitrixCrmItemFields(portalUrl, credential, entityTypeId)
    selected = [field for field in requestedFields if field in availableFields]
    return selected or ["id", "title"]


def _buildCrmItemListCommand(entityTypeId: int, selectFields: list[str], start: int) -> str:
    parameters: list[tuple[str, object]] = [
        ("entityTypeId", entityTypeId),
        ("order[id]", "DESC"),
        ("start", start),
    ]
    parameters.extend((f"select[{index}]", field) for index, field in enumerate(selectFields))
    return f"crm.item.list?{urlencode(parameters)}"


def _fetchAllCrmItemsFast(
    portalUrl: str,
    credential: str,
    entityTypeId: int,
    requestedFields: list[str],
) -> list[dict[str, object]]:
    selectFields = _selectAvailableFields(portalUrl, credential, entityTypeId, requestedFields)
    firstPayload = callBitrixRestMethod(
        portalUrl,
        credential,
        "crm.item.list",
        {
            "entityTypeId": entityTypeId,
            "select": selectFields,
            "filter": {},
            "order": {"id": "DESC"},
            "start": 0,
        },
        timeout=90,
    )
    firstResult = firstPayload.get("result") or {}
    firstItems = firstResult.get("items") if isinstance(firstResult, dict) else []
    items = [item for item in firstItems or [] if isinstance(item, dict)]
    total = int(firstPayload.get("total") or len(items))
    starts = list(range(BITRIX_PAGE_SIZE, total, BITRIX_PAGE_SIZE))

    for startChunk in _chunks(starts):
        commands = {
            f"page_{start}": _buildCrmItemListCommand(entityTypeId, selectFields, start)
            for start in startChunk
        }
        batchPayload = callBitrixRestMethod(
            portalUrl,
            credential,
            "batch",
            {"halt": 0, "cmd": commands},
            timeout=120,
        )
        batchResult = batchPayload.get("result") or {}
        commandResults = batchResult.get("result") if isinstance(batchResult, dict) else {}
        commandErrors = batchResult.get("result_error") if isinstance(batchResult, dict) else {}
        if commandErrors:
            commandName, error = next(iter(commandErrors.items()))
            raise RuntimeError(f"Bitrix CRM analysis batch failed for {commandName}: {error}")
        for start in startChunk:
            pagePayload = (commandResults or {}).get(f"page_{start}") or {}
            pageItems = pagePayload.get("items") if isinstance(pagePayload, dict) else []
            items.extend(item for item in pageItems or [] if isinstance(item, dict))

    return items


def _buildActivityListCommand(ownerTypeId: int, start: int) -> str:
    parameters = [
        ("order[ID]", "DESC"),
        ("filter[OWNER_TYPE_ID]", ownerTypeId),
        ("filter[COMPLETED]", "N"),
        ("select[0]", "ID"),
        ("select[1]", "OWNER_ID"),
        ("select[2]", "DEADLINE"),
        ("start", start),
    ]
    return f"crm.activity.list?{urlencode(parameters)}"


def _buildCompletedActivityOwnerCommand(
    ownerTypeId: int,
    ownerId: int,
    periodStart: str,
    periodEnd: str,
) -> str:
    parameters = [
        ("order[LAST_UPDATED]", "DESC"),
        ("filter[OWNER_TYPE_ID]", ownerTypeId),
        ("filter[OWNER_ID]", ownerId),
        ("filter[COMPLETED]", "Y"),
        ("filter[>=LAST_UPDATED]", periodStart),
        ("filter[<LAST_UPDATED]", periodEnd),
        ("select[0]", "ID"),
        ("select[1]", "OWNER_ID"),
        ("select[2]", "CREATED"),
        ("select[3]", "LAST_UPDATED"),
        ("select[4]", "START_TIME"),
        ("select[5]", "END_TIME"),
        ("start", 0),
    ]
    return f"crm.activity.list?{urlencode(parameters)}"


def _fetchLatestCompletedActivityTimes(
    portalUrl: str,
    credential: str,
    ownerTypeId: int,
    asOfDate: date,
    ownerIds: list[int],
) -> dict[int, str]:
    normalizedOwnerIds = sorted(
        {ownerId for ownerId in (_normalizeId(value) for value in ownerIds) if ownerId}
    )
    if not normalizedOwnerIds:
        return {}

    windowStart = asOfDate - timedelta(days=366)
    windowEnd = asOfDate + timedelta(days=1)
    periodStart = f"{windowStart.isoformat()}T00:00:00+04:00"
    periodEnd = f"{windowEnd.isoformat()}T00:00:00+04:00"
    latestByOwner: dict[int, str] = {}
    for ownerIdChunk in _chunks(normalizedOwnerIds):
        commands = {
            f"owner_{ownerId}": _buildCompletedActivityOwnerCommand(
                ownerTypeId,
                ownerId,
                periodStart,
                periodEnd,
            )
            for ownerId in ownerIdChunk
        }
        batchPayload = callBitrixRestMethod(
            portalUrl,
            credential,
            "batch",
            {"halt": 0, "cmd": commands},
            timeout=90,
        )
        batchResult = batchPayload.get("result") or {}
        commandResults = batchResult.get("result") if isinstance(batchResult, dict) else {}
        commandErrors = batchResult.get("result_error") if isinstance(batchResult, dict) else {}
        if commandErrors:
            commandName, error = next(iter(commandErrors.items()))
            raise RuntimeError(f"Bitrix completed activity batch failed for {commandName}: {error}")
        for ownerId in ownerIdChunk:
            ownerActivities = (commandResults or {}).get(f"owner_{ownerId}") or []
            for activity in ownerActivities:
                if not isinstance(activity, dict):
                    continue
                activityTime = _latestDateTimeValue(
                    activity.get("END_TIME"),
                    activity.get("START_TIME"),
                    activity.get("LAST_UPDATED"),
                    activity.get("CREATED"),
                )
                if activityTime:
                    latestByOwner[ownerId] = (
                        _latestDateTimeValue(latestByOwner.get(ownerId), activityTime) or activityTime
                    )
    return latestByOwner


def _fetchPendingActivityOwners(
    portalUrl: str,
    credential: str,
    ownerTypeId: int,
    asOfDate: date,
) -> tuple[set[int], set[int]]:
    firstPayload = callBitrixRestMethod(
        portalUrl,
        credential,
        "crm.activity.list",
        {
            "order": {"ID": "DESC"},
            "filter": {"OWNER_TYPE_ID": ownerTypeId, "COMPLETED": "N"},
            "select": ["ID", "OWNER_ID", "DEADLINE"],
            "start": 0,
        },
        timeout=90,
    )
    activities = [item for item in firstPayload.get("result") or [] if isinstance(item, dict)]
    total = int(firstPayload.get("total") or len(activities))
    starts = list(range(BITRIX_PAGE_SIZE, total, BITRIX_PAGE_SIZE))

    for startChunk in _chunks(starts):
        commands = {f"page_{start}": _buildActivityListCommand(ownerTypeId, start) for start in startChunk}
        batchPayload = callBitrixRestMethod(
            portalUrl,
            credential,
            "batch",
            {"halt": 0, "cmd": commands},
            timeout=120,
        )
        batchResult = batchPayload.get("result") or {}
        commandResults = batchResult.get("result") if isinstance(batchResult, dict) else {}
        commandErrors = batchResult.get("result_error") if isinstance(batchResult, dict) else {}
        if commandErrors:
            commandName, error = next(iter(commandErrors.items()))
            raise RuntimeError(f"Bitrix activity analysis batch failed for {commandName}: {error}")
        for start in startChunk:
            pageActivities = (commandResults or {}).get(f"page_{start}") or []
            activities.extend(item for item in pageActivities if isinstance(item, dict))

    owners: set[int] = set()
    overdueOwners: set[int] = set()
    for activity in activities:
        ownerId = _normalizeId(activity.get("OWNER_ID"))
        if not ownerId:
            continue
        owners.add(ownerId)
        deadline = _parseDateTime(activity.get("DEADLINE"))
        if deadline and deadline.date() < asOfDate:
            overdueOwners.add(ownerId)
    return owners, overdueOwners


def _fetchStatusNames(portalUrl: str, credential: str, entityId: str) -> dict[str, str]:
    payload = callBitrixRestMethod(
        portalUrl,
        credential,
        "crm.status.list",
        {"filter": {"ENTITY_ID": entityId}},
    )
    result: dict[str, str] = {}
    for item in payload.get("result") or []:
        statusId = str(item.get("STATUS_ID") or "").strip()
        name = str(item.get("NAME") or item.get("NAME_INIT") or statusId).strip()
        if statusId:
            result[statusId] = name
    return result


def _percent(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator * 100 / denominator, 1)


def _roundAmount(value: float) -> int:
    return round(value)


def _groupRows(
    items: list[dict[str, object]],
    keyResolver: Callable[[dict[str, object]], str],
    valueBuilder: Callable[[str, list[dict[str, object]]], dict[str, object]],
) -> list[dict[str, object]]:
    groups: dict[str, list[dict[str, object]]] = {}
    for item in items:
        key = keyResolver(item) or "Не указано"
        groups.setdefault(key, []).append(item)
    return [valueBuilder(key, values) for key, values in groups.items()]


def buildBitrixSalesAnalysisReport(
    portalUrl: str,
    credential: str,
    year: int,
    asOfDate: date | None = None,
) -> dict[str, object]:
    reportDate = asOfDate or date.today()
    deals = _fetchAllCrmItemsFast(
        portalUrl,
        credential,
        BITRIX_DEAL_ENTITY_TYPE_ID,
        DEAL_ANALYSIS_FIELDS,
    )
    leads = _fetchAllCrmItemsFast(
        portalUrl,
        credential,
        BITRIX_LEAD_ENTITY_TYPE_ID,
        LEAD_ANALYSIS_FIELDS,
    )

    assignedByIds = [
        userId
        for userId in (_normalizeId(item.get("assignedById")) for item in [*deals, *leads])
        if userId
    ]
    userNames = fetchBitrixUserNames(portalUrl, credential, assignedByIds)
    dealDictionaries = fetchBitrixDealDictionaries(
        portalUrl,
        credential,
        [int(item.get("categoryId") or 0) for item in deals],
    )
    dealStageNames = dealDictionaries.get("stage_names") or {}
    categoryNames = dealDictionaries.get("category_names") or {}
    leadStatusNames = _fetchStatusNames(portalUrl, credential, "STATUS")
    sourceNames = _fetchStatusNames(portalUrl, credential, "SOURCE")

    pendingDealIds, overdueDealIds = _fetchPendingActivityOwners(
        portalUrl,
        credential,
        BITRIX_DEAL_ENTITY_TYPE_ID,
        reportDate,
    )
    pendingLeadIds, overdueLeadIds = _fetchPendingActivityOwners(
        portalUrl,
        credential,
        BITRIX_LEAD_ENTITY_TYPE_ID,
        reportDate,
    )
    openDeals = [item for item in deals if _isOpen(item)]
    openLeads = [item for item in leads if _isOpen(item)]
    dealWinsYear = [item for item in deals if _semantic(item) == "S" and _dateYear(item.get("movedTime")) == year]
    dealLossesYear = [item for item in deals if _semantic(item) == "F" and _dateYear(item.get("movedTime")) == year]
    leadWinsYear = [item for item in leads if _semantic(item) == "S" and _dateYear(item.get("movedTime")) == year]
    leadLossesYear = [item for item in leads if _semantic(item) == "F" and _dateYear(item.get("movedTime")) == year]
    dealsCreatedYear = [item for item in deals if _dateYear(item.get("createdTime")) == year]
    leadsCreatedYear = [item for item in leads if _dateYear(item.get("createdTime")) == year]

    def dealStageName(item: dict[str, object]) -> str:
        stageId = _stageId(item)
        categoryId = int(item.get("categoryId") or 0)
        return str(dealStageNames.get(f"{categoryId}:{stageId}") or dealStageNames.get(stageId) or stageId or "Не указано")

    def categoryName(item: dict[str, object]) -> str:
        categoryId = int(item.get("categoryId") or 0)
        return str(categoryNames.get(categoryId) or categoryId)

    def leadStageName(item: dict[str, object]) -> str:
        stageId = _stageId(item)
        return leadStatusNames.get(stageId) or stageId or "Не указано"

    def responsibleName(item: dict[str, object]) -> str:
        userId = _normalizeId(item.get("assignedById"))
        return str(userNames.get(userId) or userId or "Не указан")

    def isStale(item: dict[str, object], days: int) -> bool:
        return _activityAge(item, reportDate) >= days

    def isControlledDeal(item: dict[str, object]) -> bool:
        return dealStageName(item).strip().casefold() not in DEAL_CONTROL_EXCLUDED_STAGES

    openLeadIds = {_normalizeId(item.get("id")) for item in openLeads}
    openLeadIds.discard(None)
    controlledOpenDeals = [item for item in openDeals if isControlledDeal(item)]
    controlledOpenDealIds = {_normalizeId(item.get("id")) for item in controlledOpenDeals}
    controlledOpenDealIds.discard(None)
    _applyLatestCompletedActivityTimes(
        controlledOpenDeals,
        _fetchLatestCompletedActivityTimes(
            portalUrl,
            credential,
            BITRIX_DEAL_ENTITY_TYPE_ID,
            reportDate,
            [int(dealId) for dealId in controlledOpenDealIds],
        ),
    )

    def buildDealGroup(name: str, values: list[dict[str, object]]) -> dict[str, object]:
        openValues = [item for item in values if _isOpen(item)]
        controlledValues = [item for item in openValues if isControlledDeal(item)]
        wins = [item for item in values if item in dealWinsYear]
        losses = [item for item in values if item in dealLossesYear]
        return {
            "name": name,
            "open_count": len(openValues),
            "open_amount": _roundAmount(sum(_asFloat(item.get("opportunity")) for item in openValues)),
            "controlled_open_count": len(controlledValues),
            "stale_30_count": sum(1 for item in controlledValues if isStale(item, 30)),
            "without_planned_activity": sum(
                1 for item in controlledValues if _normalizeId(item.get("id")) not in pendingDealIds
            ),
            "won_year": len(wins),
            "lost_year": len(losses),
            "win_rate_percent": _percent(len(wins), len(wins) + len(losses)),
        }

    pipelineRows = _groupRows(deals, categoryName, buildDealGroup)
    pipelineRows.sort(key=lambda row: (-int(row["open_amount"]), str(row["name"])))

    def buildDealStageGroup(name: str, values: list[dict[str, object]]) -> dict[str, object]:
        isExcluded = name.strip().casefold() in DEAL_CONTROL_EXCLUDED_STAGES
        controlledValues = [] if isExcluded else values
        return {
            "name": name,
            "excluded_from_control": isExcluded,
            "open_count": len(values),
            "open_amount": _roundAmount(sum(_asFloat(item.get("opportunity")) for item in values)),
            "stale_30_count": sum(1 for item in controlledValues if isStale(item, 30)),
            "stale_60_count": sum(1 for item in controlledValues if isStale(item, 60)),
            "without_planned_activity": sum(
                1 for item in controlledValues if _normalizeId(item.get("id")) not in pendingDealIds
            ),
        }

    dealStageRows = _groupRows(
        openDeals,
        dealStageName,
        buildDealStageGroup,
    )
    dealStageRows.sort(key=lambda row: (-int(row["open_amount"]), str(row["name"])))

    dealResponsibleRows = _groupRows(deals, responsibleName, buildDealGroup)
    dealResponsibleRows.sort(key=lambda row: (-int(row["open_amount"]), str(row["name"])))

    def buildLeadGroup(name: str, values: list[dict[str, object]]) -> dict[str, object]:
        activeValues = [item for item in values if _isOpen(item)]
        wins = [item for item in values if item in leadWinsYear]
        losses = [item for item in values if item in leadLossesYear]
        return {
            "name": name,
            "active_count": len(activeValues),
            "stale_7_count": sum(1 for item in activeValues if isStale(item, 7)),
            "stale_30_count": sum(1 for item in activeValues if isStale(item, 30)),
            "without_planned_activity": sum(
                1 for item in activeValues if _normalizeId(item.get("id")) not in pendingLeadIds
            ),
            "converted_year": len(wins),
            "lost_year": len(losses),
            "conversion_percent": _percent(len(wins), len(wins) + len(losses)),
        }

    leadStatusRows = _groupRows(openLeads, leadStageName, buildLeadGroup)
    leadStatusRows.sort(key=lambda row: (-int(row["active_count"]), str(row["name"])))
    leadResponsibleRows = _groupRows(leads, responsibleName, buildLeadGroup)
    leadResponsibleRows.sort(key=lambda row: (-int(row["active_count"]), str(row["name"])))

    leadSourceRows = _groupRows(
        leadsCreatedYear,
        lambda item: sourceNames.get(str(item.get("sourceId") or "")) or str(item.get("sourceId") or "Не указан"),
        lambda name, values: {
            "name": name,
            "created": len(values),
            "converted": sum(1 for item in values if _semantic(item) == "S"),
            "lost": sum(1 for item in values if _semantic(item) == "F"),
            "active": sum(1 for item in values if _isOpen(item)),
            "conversion_percent": _percent(
                sum(1 for item in values if _semantic(item) == "S"),
                len(values),
            ),
        },
    )
    leadSourceRows.sort(key=lambda row: (-int(row["created"]), str(row["name"])))

    def dealDetails(item: dict[str, object]) -> dict[str, object]:
        dealId = _normalizeId(item.get("id"))
        return {
            "id": dealId,
            "title": item.get("title") or "",
            "pipeline": categoryName(item),
            "stage": dealStageName(item),
            "responsible": responsibleName(item),
            "amount": _roundAmount(_asFloat(item.get("opportunity"))),
            "days_since_activity": _activityAge(item, reportDate),
            "last_activity_time": _activityTime(item),
            "has_planned_activity": dealId in pendingDealIds,
            "has_overdue_activity": dealId in overdueDealIds,
            "problem_reasons": dealProblemReasons(item),
            "url": f"{portalUrl.rstrip('/')}/crm/deal/details/{dealId}/" if dealId else "",
        }

    def leadDetails(item: dict[str, object]) -> dict[str, object]:
        leadId = _normalizeId(item.get("id"))
        return {
            "id": leadId,
            "title": item.get("title") or "",
            "status": leadStageName(item),
            "source": sourceNames.get(str(item.get("sourceId") or "")) or str(item.get("sourceId") or "Не указан"),
            "responsible": responsibleName(item),
            "days_since_activity": _activityAge(item, reportDate),
            "last_activity_time": _activityTime(item),
            "has_planned_activity": leadId in pendingLeadIds,
            "has_overdue_activity": leadId in overdueLeadIds,
            "problem_reasons": leadProblemReasons(item),
            "url": f"{portalUrl.rstrip('/')}/crm/lead/details/{leadId}/" if leadId else "",
        }

    def dealProblemReasons(item: dict[str, object]) -> list[str]:
        dealId = _normalizeId(item.get("id"))
        reasons: list[str] = []
        if isStale(item, 30):
            reasons.append(f"Нет активности {_activityAge(item, reportDate)} дн.")
        if dealId not in pendingDealIds:
            reasons.append("Нет следующего действия")
        if dealId in overdueDealIds:
            reasons.append("Есть просроченная активность")
        return reasons

    def leadProblemReasons(item: dict[str, object]) -> list[str]:
        leadId = _normalizeId(item.get("id"))
        reasons: list[str] = []
        if isStale(item, 7):
            reasons.append(f"Нет активности {_activityAge(item, reportDate)} дн.")
        if leadId not in pendingLeadIds:
            reasons.append("Нет следующего действия")
        if leadId in overdueLeadIds:
            reasons.append("Есть просроченная активность")
        return reasons

    staleDeals = sorted(
        [item for item in controlledOpenDeals if isStale(item, 30)],
        key=lambda item: (_asFloat(item.get("opportunity")), _activityAge(item, reportDate)),
        reverse=True,
    )
    staleLeads = sorted(
        [item for item in openLeads if isStale(item, 7)],
        key=lambda item: _activityAge(item, reportDate),
        reverse=True,
    )
    overdueDeals = [item for item in controlledOpenDeals if _normalizeId(item.get("id")) in overdueDealIds]
    overdueDeals.sort(key=lambda item: _asFloat(item.get("opportunity")), reverse=True)

    priorityByResponsible: dict[str, dict[str, list[dict[str, object]]]] = {}
    for name in sorted({responsibleName(item) for item in [*controlledOpenDeals, *openLeads]}):
        problemDeals = [
            item
            for item in controlledOpenDeals
            if responsibleName(item) == name and dealProblemReasons(item)
        ]
        problemDeals.sort(
            key=lambda item: (
                _asFloat(item.get("opportunity")),
                len(dealProblemReasons(item)),
                _activityAge(item, reportDate),
            ),
            reverse=True,
        )
        problemLeads = [
            item
            for item in openLeads
            if responsibleName(item) == name and leadProblemReasons(item)
        ]
        problemLeads.sort(
            key=lambda item: (
                1 if str(item.get("sourceId") or "") == "WEB" else 0,
                1 if _normalizeId(item.get("id")) not in pendingLeadIds else 0,
                1 if _normalizeId(item.get("id")) in overdueLeadIds else 0,
                _activityAge(item, reportDate),
            ),
            reverse=True,
        )
        priorityByResponsible[name] = {
            "deals": [dealDetails(item) for item in problemDeals[:20]],
            "leads": [leadDetails(item) for item in problemLeads[:20]],
        }

    dealClosedYearCount = len(dealWinsYear) + len(dealLossesYear)
    leadClosedYearCount = len(leadWinsYear) + len(leadLossesYear)
    return {
        "year": int(year),
        "as_of_date": reportDate.isoformat(),
        "methodology": {
            "closed_event_date": "movedTime",
            "staleness_date": "max(lastActivityTime, latest completed crm.activity), with createdTime fallback",
            "planned_activity": "crm.activity.list with COMPLETED=N",
            "deal_control_excluded_stages": sorted(DEAL_CONTROL_EXCLUDED_STAGES),
        },
        "priority_by_responsible": priorityByResponsible,
        "deals": {
            "total": len(deals),
            "created_year": len(dealsCreatedYear),
            "open_count": len(openDeals),
            "open_amount": _roundAmount(sum(_asFloat(item.get("opportunity")) for item in openDeals)),
            "controlled_open_count": len(controlledOpenDeals),
            "controlled_open_amount": _roundAmount(
                sum(_asFloat(item.get("opportunity")) for item in controlledOpenDeals)
            ),
            "won_year": len(dealWinsYear),
            "won_amount_year": _roundAmount(sum(_asFloat(item.get("opportunity")) for item in dealWinsYear)),
            "lost_year": len(dealLossesYear),
            "lost_amount_year": _roundAmount(sum(_asFloat(item.get("opportunity")) for item in dealLossesYear)),
            "closed_year": dealClosedYearCount,
            "win_rate_percent": _percent(len(dealWinsYear), dealClosedYearCount),
            "stale_30_count": sum(1 for item in controlledOpenDeals if isStale(item, 30)),
            "stale_30_amount": _roundAmount(
                sum(_asFloat(item.get("opportunity")) for item in controlledOpenDeals if isStale(item, 30))
            ),
            "stale_60_count": sum(1 for item in controlledOpenDeals if isStale(item, 60)),
            "stale_90_count": sum(1 for item in controlledOpenDeals if isStale(item, 90)),
            "without_planned_activity": sum(
                1 for item in controlledOpenDeals if _normalizeId(item.get("id")) not in pendingDealIds
            ),
            "with_overdue_activity": len(controlledOpenDealIds & overdueDealIds),
            "zero_amount_count": sum(1 for item in openDeals if _asFloat(item.get("opportunity")) <= 0),
            "without_company_count": sum(1 for item in openDeals if not _normalizeId(item.get("companyId"))),
            "pipelines": pipelineRows,
            "stages": dealStageRows,
            "responsibles": dealResponsibleRows,
            "top_stale": [dealDetails(item) for item in staleDeals[:25]],
            "top_overdue": [dealDetails(item) for item in overdueDeals[:25]],
        },
        "leads": {
            "total": len(leads),
            "created_year": len(leadsCreatedYear),
            "active_count": len(openLeads),
            "converted_year": len(leadWinsYear),
            "lost_year": len(leadLossesYear),
            "closed_year": leadClosedYearCount,
            "conversion_percent": _percent(len(leadWinsYear), leadClosedYearCount),
            "stale_7_count": sum(1 for item in openLeads if isStale(item, 7)),
            "stale_30_count": sum(1 for item in openLeads if isStale(item, 30)),
            "stale_90_count": sum(1 for item in openLeads if isStale(item, 90)),
            "without_planned_activity": sum(
                1 for item in openLeads if _normalizeId(item.get("id")) not in pendingLeadIds
            ),
            "with_overdue_activity": len(openLeadIds & overdueLeadIds),
            "without_phone_and_email": sum(
                1 for item in openLeads if not _asBool(item.get("hasPhone")) and not _asBool(item.get("hasEmail"))
            ),
            "without_source": sum(1 for item in openLeads if not str(item.get("sourceId") or "").strip()),
            "statuses": leadStatusRows,
            "sources_created_year": leadSourceRows,
            "responsibles": leadResponsibleRows,
            "top_stale": [leadDetails(item) for item in staleLeads[:25]],
        },
    }
