from dataclasses import dataclass
import re
from time import perf_counter

import requests

BITRIX_DEAL_ENTITY_TYPE_ID = 2
BITRIX_LEAD_ENTITY_TYPE_ID = 1
BITRIX_INVOICE_ENTITY_TYPE_ID = 31
BITRIX_PAGE_SIZE = 50
BITRIX_CAPTURE_BATCH_SIZE = 500
BITRIX_PLACEHOLDER_VALUES = {
    "replace_me",
    "put_incoming_webhook_or_oauth_token_here",
    "set_me_in_render_dashboard",
}
BITRIX_DEALS_SELECT_FIELDS = [
    "id",
    "title",
    "stageId",
    "assignedById",
    "opportunity",
    "currencyId",
    "companyId",
    "categoryId",
    "createdTime",
    "updatedTime",
]
BITRIX_CRM_COMMON_SELECT_FIELDS = [
    "id",
    "title",
    "stageId",
    "statusId",
    "assignedById",
    "opportunity",
    "currencyId",
    "companyId",
    "createdTime",
    "updatedTime",
]
BITRIX_INVOICE_SELECT_FIELDS = [
    *BITRIX_CRM_COMMON_SELECT_FIELDS,
    "categoryId",
    "begindate",
    "closedate",
    "parentId2",
]
BITRIX_INVOICE_EXTRA_FIELD_LABELS = {
    "kot_products": "КОТ ПРОДУКТЫ",
    "products": "Продукты",
    "energy_products": "Продукты (энергетика)",
    "stage_group": "Группа стадий",
    "pipeline_stage_invoice": "Воронка/стадия/счет",
}
_BITRIX_CRM_FIELD_CACHE: dict[tuple[str, str, int], dict[str, object]] = {}


@dataclass(frozen=True)
class BitrixRestContext:
    endpoint: str
    authMode: str
    defaultPayload: dict[str, object]


def _buildBitrixWebhookEndpoint(webhookUrl: str, method: str) -> str:
    endpoint = webhookUrl.strip().rstrip("/")
    lastSegment = endpoint.rsplit("/", 1)[-1]
    if "." in lastSegment:
        return f"{endpoint.rsplit('/', 1)[0]}/{method}"
    return f"{endpoint}/{method}"


def buildBitrixRestContext(portalUrl: str, credential: str, method: str = "crm.item.list") -> BitrixRestContext:
    portalUrlNormalized = str(portalUrl or "").strip().rstrip("/")
    credentialNormalized = str(credential or "").strip()
    methodNormalized = str(method or "").strip().lstrip("/")

    if not portalUrlNormalized:
        raise RuntimeError("BITRIX_PORTAL_URL is not set")
    if not methodNormalized:
        raise RuntimeError("Bitrix REST method is not set")

    if not credentialNormalized or credentialNormalized in BITRIX_PLACEHOLDER_VALUES:
        raise RuntimeError(
            "Btrx is not set. Put an incoming webhook or OAuth token into the Render variable Btrx."
        )

    if credentialNormalized.startswith("http://") or credentialNormalized.startswith("https://"):
        endpoint = _buildBitrixWebhookEndpoint(credentialNormalized, methodNormalized)
        return BitrixRestContext(endpoint=endpoint, authMode="webhook_url", defaultPayload={})

    if "/" in credentialNormalized and " " not in credentialNormalized:
        endpoint = f"{portalUrlNormalized}/rest/{credentialNormalized.strip('/')}/{methodNormalized}"
        return BitrixRestContext(endpoint=endpoint, authMode="webhook_path", defaultPayload={})

    return BitrixRestContext(
        endpoint=f"{portalUrlNormalized}/rest/{methodNormalized}",
        authMode="oauth_token",
        defaultPayload={"auth": credentialNormalized},
    )


def buildBitrixDealsFilter(
    search: str | None = None,
    stageId: str | None = None,
    assignedById: int | None = None,
    categoryId: int | None = None,
) -> dict[str, object]:
    filters: dict[str, object] = {}

    if search:
        filters["%title"] = search.strip()
    if stageId:
        filters["stageId"] = stageId.strip()
    if assignedById is not None:
        filters["assignedById"] = assignedById
    if categoryId is not None:
        filters["categoryId"] = categoryId

    return filters


def extractBitrixError(payload: dict[str, object]) -> str | None:
    errorCode = str(payload.get("error") or "").strip()
    errorDescription = str(payload.get("error_description") or "").strip()

    if not errorCode and not errorDescription:
        return None

    detail = errorDescription or errorCode or "Bitrix request failed"
    if errorCode == "INVALID_CREDENTIALS":
        return (
            f"{detail}. Bitrix24 REST API accepts an incoming webhook or OAuth token in Btrx; "
            "application passwords are not suitable for crm.item.list."
        )

    return detail


def callBitrixRestMethod(
    portalUrl: str,
    credential: str,
    method: str,
    payload: dict[str, object] | None = None,
    timeout: int = 45,
) -> dict[str, object]:
    restContext = buildBitrixRestContext(portalUrl, credential, method=method)
    response = requests.post(
        restContext.endpoint,
        json={**restContext.defaultPayload, **dict(payload or {})},
        timeout=timeout,
    )
    response.raise_for_status()

    responsePayload = response.json()
    responseError = extractBitrixError(responsePayload)
    if responseError is not None:
        raise RuntimeError(responseError)
    return responsePayload


def callBitrixRestMethodGet(
    portalUrl: str,
    credential: str,
    method: str,
    payload: dict[str, object] | None = None,
    timeout: int = 45,
) -> dict[str, object]:
    restContext = buildBitrixRestContext(portalUrl, credential, method=method)
    response = requests.get(
        restContext.endpoint,
        params={**restContext.defaultPayload, **dict(payload or {})},
        timeout=timeout,
    )
    response.raise_for_status()

    responsePayload = response.json()
    responseError = extractBitrixError(responsePayload)
    if responseError is not None:
        raise RuntimeError(responseError)
    return responsePayload


def normalizeBitrixFieldLabel(value: object) -> str:
    return re.sub(r"[^0-9a-zа-я]+", "", str(value or "").lower().replace("ё", "е"))


def fetchBitrixCrmItemFields(portalUrl: str, credential: str, entityTypeId: int) -> dict[str, object]:
    cacheKey = (str(portalUrl or "").strip().rstrip("/"), str(credential or "").strip(), int(entityTypeId))
    if cacheKey in _BITRIX_CRM_FIELD_CACHE:
        return _BITRIX_CRM_FIELD_CACHE[cacheKey]

    responsePayload = callBitrixRestMethod(
        portalUrl,
        credential,
        "crm.item.fields",
        {"entityTypeId": int(entityTypeId)},
        timeout=45,
    )
    resultPayload = responsePayload.get("result") or {}
    fieldsPayload = {}
    if isinstance(resultPayload, dict):
        fieldsPayload = resultPayload.get("fields") if "fields" in resultPayload else resultPayload
    fields = fieldsPayload if isinstance(fieldsPayload, dict) else {}
    _BITRIX_CRM_FIELD_CACHE[cacheKey] = fields
    return fields


def extractBitrixFieldLabels(fieldInfo: object) -> list[str]:
    if not isinstance(fieldInfo, dict):
        return []

    labels: list[str] = []
    for key in ("title", "listLabel", "formLabel", "filterLabel", "name", "label"):
        value = fieldInfo.get(key)
        if value:
            labels.append(str(value))

    settings = fieldInfo.get("settings")
    if isinstance(settings, dict):
        for key in ("title", "listLabel", "formLabel", "filterLabel", "label"):
            value = settings.get(key)
            if value:
                labels.append(str(value))

    return labels


def buildBitrixFieldValueMap(fieldInfo: object) -> dict[object, str]:
    if not isinstance(fieldInfo, dict):
        return {}

    itemsPayload = fieldInfo.get("items") or fieldInfo.get("values") or []
    valueMap: dict[object, str] = {}
    if not isinstance(itemsPayload, list):
        return valueMap

    for item in itemsPayload:
        if not isinstance(item, dict):
            continue
        itemId = item.get("ID") or item.get("id") or item.get("VALUE_ID") or item.get("valueId")
        name = item.get("VALUE") or item.get("value") or item.get("NAME") or item.get("name") or item.get("TITLE") or item.get("title")
        if itemId is not None and name:
            valueMap[str(itemId)] = str(name)
            valueMap[itemId] = str(name)
    return valueMap


def resolveBitrixInvoiceExtraFields(portalUrl: str, credential: str) -> dict[str, object]:
    try:
        fields = fetchBitrixCrmItemFields(portalUrl, credential, BITRIX_INVOICE_ENTITY_TYPE_ID)
    except Exception:
        return {
            "invoice_extra_field_names": {},
            "invoice_extra_field_value_maps": {},
        }
    targetLabelsByKey = {
        key: normalizeBitrixFieldLabel(label)
        for key, label in BITRIX_INVOICE_EXTRA_FIELD_LABELS.items()
    }
    fieldNames: dict[str, str] = {}
    valueMaps: dict[str, dict[object, str]] = {}

    for fieldName, fieldInfo in fields.items():
        normalizedLabels = {normalizeBitrixFieldLabel(label) for label in extractBitrixFieldLabels(fieldInfo)}
        for key, normalizedTarget in targetLabelsByKey.items():
            if key in fieldNames:
                continue
            if normalizedTarget in normalizedLabels:
                fieldNames[key] = str(fieldName)
                valueMaps[key] = buildBitrixFieldValueMap(fieldInfo)

    return {
        "invoice_extra_field_names": fieldNames,
        "invoice_extra_field_value_maps": valueMaps,
    }


def buildBitrixInvoiceSelectFields(portalUrl: str, credential: str) -> tuple[list[str], dict[str, object]]:
    extraFieldInfo = resolveBitrixInvoiceExtraFields(portalUrl, credential)
    selectFields = list(BITRIX_INVOICE_SELECT_FIELDS)
    for fieldName in (extraFieldInfo.get("invoice_extra_field_names") or {}).values():
        if isinstance(fieldName, str) and fieldName and fieldName not in selectFields:
            selectFields.append(fieldName)
    return selectFields, extraFieldInfo


def fetchBitrixDeals(
    portalUrl: str,
    credential: str,
    limit: int = 20,
    search: str | None = None,
    stageId: str | None = None,
    assignedById: int | None = None,
    categoryId: int | None = None,
) -> dict[str, object]:
    requestedLimit = max(1, min(int(limit), 500))
    restContext = buildBitrixRestContext(portalUrl, credential)
    filters = buildBitrixDealsFilter(
        search=search,
        stageId=stageId,
        assignedById=assignedById,
        categoryId=categoryId,
    )

    items: list[dict[str, object]] = []
    start = 0
    total = 0

    while len(items) < requestedLimit:
        payload = {
            **restContext.defaultPayload,
            "entityTypeId": BITRIX_DEAL_ENTITY_TYPE_ID,
            "select": BITRIX_DEALS_SELECT_FIELDS,
            "filter": filters,
            "order": {"id": "DESC"},
            "start": start,
        }
        responsePayload = callBitrixRestMethod(
            portalUrl,
            credential,
            "crm.item.list",
            payload,
            timeout=45,
        )

        resultPayload = responsePayload.get("result") or {}
        pageItems = resultPayload.get("items") or []
        total = int(responsePayload.get("total") or total or len(pageItems))

        remainingItems = requestedLimit - len(items)
        items.extend(pageItems[:remainingItems])

        nextStart = resultPayload.get("next", responsePayload.get("next"))
        if nextStart is None or not pageItems:
            break
        start = int(nextStart)

        # The Bitrix page size is fixed at 50, so if we got fewer than that, the list is exhausted.
        if len(pageItems) < BITRIX_PAGE_SIZE:
            break

    return {
        "portal_url": portalUrl.rstrip("/"),
        "auth_mode": restContext.authMode,
        "items": items,
        "total": total,
        "requested_limit": requestedLimit,
        "filter": filters,
    }


def fetchAllBitrixDeals(portalUrl: str, credential: str) -> dict[str, object]:
    return fetchAllBitrixCrmItems(
        portalUrl,
        credential,
        entityTypeId=BITRIX_DEAL_ENTITY_TYPE_ID,
        selectFields=BITRIX_DEALS_SELECT_FIELDS,
    )


def fetchAllBitrixCrmItems(
    portalUrl: str,
    credential: str,
    *,
    entityTypeId: int,
    selectFields: list[str] | None = None,
) -> dict[str, object]:
    restContext = buildBitrixRestContext(portalUrl, credential)
    items: list[dict[str, object]] = []
    start = 0
    total = 0

    while True:
        payload = {
            **restContext.defaultPayload,
            "entityTypeId": entityTypeId,
            "select": selectFields or BITRIX_CRM_COMMON_SELECT_FIELDS,
            "filter": {},
            "order": {"id": "DESC"},
            "start": start,
        }
        responsePayload = callBitrixRestMethod(
            portalUrl,
            credential,
            "crm.item.list",
            payload,
            timeout=90,
        )

        resultPayload = responsePayload.get("result") or {}
        pageItems = resultPayload.get("items") or []
        items.extend(pageItems)
        total = int(responsePayload.get("total") or total or len(items))

        nextStart = resultPayload.get("next", responsePayload.get("next"))
        if nextStart is None or not pageItems:
            break
        start = int(nextStart)

        if len(pageItems) < BITRIX_PAGE_SIZE:
            break

    return {
        "portal_url": portalUrl.rstrip("/"),
        "auth_mode": restContext.authMode,
        "items": items,
        "total": total or len(items),
    }


def fetchBitrixCrmItemsPage(
    portalUrl: str,
    credential: str,
    *,
    entityTypeId: int,
    start: int = 0,
    selectFields: list[str] | None = None,
    batchSize: int = BITRIX_CAPTURE_BATCH_SIZE,
) -> dict[str, object]:
    restContext = buildBitrixRestContext(portalUrl, credential)
    requestedBatchSize = max(BITRIX_PAGE_SIZE, min(int(batchSize or BITRIX_CAPTURE_BATCH_SIZE), BITRIX_CAPTURE_BATCH_SIZE))
    items: list[dict[str, object]] = []
    currentStart = max(0, int(start or 0))
    total = 0
    nextStart = currentStart
    trace: list[dict[str, object]] = []

    while len(items) < requestedBatchSize and nextStart is not None:
        pageStartedAt = perf_counter()
        responsePayload = callBitrixRestMethod(
            portalUrl,
            credential,
            "crm.item.list",
            {
                **restContext.defaultPayload,
                "entityTypeId": entityTypeId,
                "select": selectFields or BITRIX_CRM_COMMON_SELECT_FIELDS,
                "filter": {},
                "order": {"id": "DESC"},
                "start": currentStart,
            },
            timeout=30,
        )
        resultPayload = responsePayload.get("result") or {}
        pageItems = resultPayload.get("items") or []
        items.extend(pageItems)
        total = int(responsePayload.get("total") or total or len(items))
        nextStart = resultPayload.get("next", responsePayload.get("next"))
        trace.append(
            {
                "start": currentStart,
                "items": len(pageItems),
                "next": nextStart,
                "duration_seconds": round(perf_counter() - pageStartedAt, 3),
            }
        )
        if nextStart is None or not pageItems or len(pageItems) < BITRIX_PAGE_SIZE:
            break
        currentStart = int(nextStart)

    return {
        "portal_url": portalUrl.rstrip("/"),
        "auth_mode": restContext.authMode,
        "items": items,
        "total": total or len(items),
        "next": nextStart,
        "start": start,
        "trace": trace,
    }


def fetchBitrixDealsPage(portalUrl: str, credential: str, start: int = 0) -> dict[str, object]:
    return fetchBitrixCrmItemsPage(
        portalUrl,
        credential,
        entityTypeId=BITRIX_DEAL_ENTITY_TYPE_ID,
        selectFields=BITRIX_DEALS_SELECT_FIELDS,
        start=start,
    )


def fetchBitrixLeadsPage(portalUrl: str, credential: str, start: int = 0) -> dict[str, object]:
    return fetchBitrixCrmItemsPage(
        portalUrl,
        credential,
        entityTypeId=BITRIX_LEAD_ENTITY_TYPE_ID,
        selectFields=BITRIX_CRM_COMMON_SELECT_FIELDS,
        start=start,
    )


def fetchBitrixInvoicesPage(portalUrl: str, credential: str, start: int = 0) -> dict[str, object]:
    selectFields, extraFieldInfo = buildBitrixInvoiceSelectFields(portalUrl, credential)
    payload = fetchBitrixCrmItemsPage(
        portalUrl,
        credential,
        entityTypeId=BITRIX_INVOICE_ENTITY_TYPE_ID,
        selectFields=selectFields,
        start=start,
    )
    payload.update(extraFieldInfo)
    return payload


def fetchAllBitrixLeads(portalUrl: str, credential: str) -> dict[str, object]:
    return fetchAllBitrixCrmItems(
        portalUrl,
        credential,
        entityTypeId=BITRIX_LEAD_ENTITY_TYPE_ID,
        selectFields=BITRIX_CRM_COMMON_SELECT_FIELDS,
    )


def fetchAllBitrixInvoices(portalUrl: str, credential: str) -> dict[str, object]:
    selectFields, extraFieldInfo = buildBitrixInvoiceSelectFields(portalUrl, credential)
    payload = fetchAllBitrixCrmItems(
        portalUrl,
        credential,
        entityTypeId=BITRIX_INVOICE_ENTITY_TYPE_ID,
        selectFields=selectFields,
    )
    payload.update(extraFieldInfo)
    return payload


def fetchBitrixDealDictionaries(
    portalUrl: str,
    credential: str,
    categoryIds: list[int],
    assignedByIds: list[int] | None = None,
    companyIds: list[int] | None = None,
) -> dict[str, dict[object, str]]:
    stageNames: dict[object, str] = {}
    categoryNames: dict[object, str] = {0: "Общая воронка"}
    assignedByNames: dict[object, str] = {}

    try:
        categoryPayload = callBitrixRestMethod(
            portalUrl,
            credential,
            "crm.category.list",
            {"entityTypeId": BITRIX_DEAL_ENTITY_TYPE_ID},
        )
        categoryResult = categoryPayload.get("result") or {}
        categories = categoryResult.get("categories") if isinstance(categoryResult, dict) else categoryResult
        for category in categories or []:
            categoryId = category.get("id")
            name = category.get("name") or category.get("title")
            if categoryId is not None and name:
                categoryNames[int(categoryId)] = str(name)
    except Exception:
        pass

    uniqueCategoryIds = sorted({int(value or 0) for value in categoryIds} | {0})
    for categoryId in uniqueCategoryIds:
        entityId = "DEAL_STAGE" if categoryId == 0 else f"DEAL_STAGE_{categoryId}"
        try:
            stagePayload = callBitrixRestMethod(
                portalUrl,
                credential,
                "crm.status.list",
                {"filter": {"ENTITY_ID": entityId}},
            )
        except Exception:
            continue

        stages = stagePayload.get("result") or []
        for stage in stages:
            statusId = stage.get("STATUS_ID")
            name = stage.get("NAME") or stage.get("NAME_INIT") or statusId
            if statusId and name:
                stageNames[str(statusId)] = str(name)
                stageNames[f"{categoryId}:{statusId}"] = str(name)

    assignedByNames = fetchBitrixUserNames(portalUrl, credential, assignedByIds or [])

    return {
        "stage_names": stageNames,
        "category_names": categoryNames,
        "assigned_by_names": assignedByNames,
        "company_names": {},
    }


def fetchBitrixUserNames(portalUrl: str, credential: str, userIds: list[int]) -> dict[object, str]:
    uniqueUserIds: list[int] = []
    for value in userIds:
        try:
            userId = int(value or 0)
        except (TypeError, ValueError):
            continue
        if userId > 0:
            uniqueUserIds.append(userId)

    uniqueUserIds = sorted(set(uniqueUserIds))
    if not uniqueUserIds:
        return {}

    userNames: dict[object, str] = {}
    neededIds = set(uniqueUserIds)
    for payload in (
        {"filter": {"ID": uniqueUserIds}},
        {"FILTER": {"ID": uniqueUserIds}},
    ):
        try:
            usersPayload = callBitrixRestMethod(portalUrl, credential, "user.get", payload)
        except Exception:
            continue
        userNames.update(extractBitrixUserNames(usersPayload.get("result") or [], neededIds))
        if neededIds <= set(userNames):
            return userNames

    missingUserIds = [userId for userId in uniqueUserIds if userId not in userNames]
    if missingUserIds:
        userNames.update(fetchBitrixAllUserNames(portalUrl, credential, missingUserIds))

    missingUserIds = [userId for userId in uniqueUserIds if userId not in userNames]
    for userId in missingUserIds:
        for payload in (
            {"filter": {"ID": userId}},
            {"FILTER": {"ID": userId}},
            {"ID": userId},
        ):
            try:
                usersPayload = callBitrixRestMethod(portalUrl, credential, "user.get", payload)
            except Exception:
                continue
            userNames.update(extractBitrixUserNames(usersPayload.get("result") or [], {userId}))
            if userId in userNames:
                break

    return userNames


def extractBitrixUserNames(users: list[dict[str, object]], neededIds: set[int]) -> dict[object, str]:
    userNames: dict[object, str] = {}
    for user in users:
        resultUserId = user.get("ID") or user.get("id")
        try:
            normalizedUserId = int(resultUserId or 0)
        except (TypeError, ValueError):
            continue
        if normalizedUserId not in neededIds:
            continue
        displayName = formatBitrixUserDisplayName(user)
        if displayName:
            userNames[normalizedUserId] = displayName
    return userNames


def formatBitrixUserDisplayName(user: dict[str, object]) -> str:
    resultUserId = user.get("ID") or user.get("id")
    lastName = str(user.get("LAST_NAME") or "").strip()
    name = str(user.get("NAME") or "").strip()
    secondName = str(user.get("SECOND_NAME") or "").strip()
    displayName = " ".join(part for part in [lastName, name, secondName] if part)
    return displayName or str(user.get("LOGIN") or "").strip() or str(resultUserId or "").strip()


def normalizeBitrixUser(user: dict[str, object]) -> dict[str, object]:
    resultUserId = user.get("ID") or user.get("id")
    try:
        normalizedUserId = int(resultUserId or 0)
    except (TypeError, ValueError):
        normalizedUserId = 0

    return {
        "id": normalizedUserId or resultUserId,
        "name": formatBitrixUserDisplayName(user),
        "last_name": user.get("LAST_NAME") or user.get("lastName"),
        "first_name": user.get("NAME") or user.get("name"),
        "second_name": user.get("SECOND_NAME") or user.get("secondName"),
        "login": user.get("LOGIN") or user.get("login"),
        "email": user.get("EMAIL") or user.get("email"),
        "active": user.get("ACTIVE") if "ACTIVE" in user else user.get("active"),
        "work_position": user.get("WORK_POSITION") or user.get("workPosition"),
    }


def fetchBitrixUsers(portalUrl: str, credential: str, limit: int = 1000) -> dict[str, object]:
    restContext = buildBitrixRestContext(portalUrl, credential, method="user.get")
    requestedLimit = max(1, min(int(limit or 1000), 5000))
    users: list[dict[str, object]] = []
    start = 0

    while len(users) < requestedLimit:
        requestPayload = {} if start == 0 else {"start": start}
        usersPayload = callBitrixRestMethodGet(
            portalUrl,
            credential,
            "user.get",
            requestPayload,
        )
        pageUsers = usersPayload.get("result") or []
        if not pageUsers:
            break

        for user in pageUsers:
            if isinstance(user, dict):
                users.append(normalizeBitrixUser(user))
                if len(users) >= requestedLimit:
                    break

        nextStart = usersPayload.get("next")
        if nextStart is None or len(pageUsers) < BITRIX_PAGE_SIZE:
            break
        start = int(nextStart)

    return {
        "portal_url": portalUrl.rstrip("/"),
        "auth_mode": restContext.authMode,
        "users": users,
        "total": len(users),
    }


def normalizeBitrixCompany(company: dict[str, object]) -> dict[str, object]:
    companyId = company.get("ID") or company.get("id")
    try:
        normalizedCompanyId = int(companyId or 0)
    except (TypeError, ValueError):
        normalizedCompanyId = 0

    return {
        "id": normalizedCompanyId or companyId,
        "title": company.get("TITLE") or company.get("title") or "",
        "raw": company,
    }


def fetchBitrixCompanies(portalUrl: str, credential: str, limit: int = 5000) -> dict[str, object]:
    restContext = buildBitrixRestContext(portalUrl, credential, method="crm.company.list")
    requestedLimit = max(1, min(int(limit or 5000), 20000))
    companies: list[dict[str, object]] = []
    start = 0

    while len(companies) < requestedLimit:
        companyPayload = callBitrixRestMethod(
            portalUrl,
            credential,
            "crm.company.list",
            {
                "select": ["ID", "TITLE"],
                "order": {"ID": "ASC"},
                "start": start,
            },
            timeout=90,
        )
        pageCompanies = companyPayload.get("result") or []
        if not pageCompanies:
            break

        for company in pageCompanies:
            if isinstance(company, dict):
                normalizedCompany = normalizeBitrixCompany(company)
                if normalizedCompany.get("id") and normalizedCompany.get("title"):
                    companies.append(normalizedCompany)
                if len(companies) >= requestedLimit:
                    break

        nextStart = companyPayload.get("next")
        if nextStart is None or len(pageCompanies) < BITRIX_PAGE_SIZE:
            break
        start = int(nextStart)

    return {
        "portal_url": portalUrl.rstrip("/"),
        "auth_mode": restContext.authMode,
        "companies": companies,
        "total": len(companies),
    }


def fetchBitrixAllUserNames(portalUrl: str, credential: str, neededUserIds: list[int]) -> dict[object, str]:
    neededIds = set(neededUserIds)
    userNames: dict[object, str] = {}
    start = 0
    while neededIds - set(userNames):
        try:
            usersPayload = callBitrixRestMethod(
                portalUrl,
                credential,
                "user.get",
                {"start": start},
            )
        except Exception:
            break

        users = usersPayload.get("result") or []
        if not users:
            break

        userNames.update(extractBitrixUserNames(users, neededIds))

        nextStart = usersPayload.get("next")
        if nextStart is None or len(users) < BITRIX_PAGE_SIZE:
            break
        start = int(nextStart)

    return userNames


def fetchBitrixCompanyNames(portalUrl: str, credential: str, companyIds: list[int]) -> dict[object, str]:
    companyNames: dict[object, str] = {}
    uniqueCompanyIds: list[int] = []
    for value in companyIds:
        try:
            companyId = int(value or 0)
        except (TypeError, ValueError):
            continue
        if companyId > 0:
            uniqueCompanyIds.append(companyId)

    uniqueCompanyIds = sorted(set(uniqueCompanyIds))
    for companyId in uniqueCompanyIds:
        try:
            companyPayload = callBitrixRestMethod(
                portalUrl,
                credential,
                "crm.company.get",
                {"id": companyId},
            )
        except Exception:
            continue

        company = companyPayload.get("result") or {}
        title = str(company.get("TITLE") or company.get("title") or "").strip()
        if title:
            companyNames[companyId] = title

    return companyNames


def fetchBitrixCrmCategoryNames(portalUrl: str, credential: str, entityTypeId: int) -> dict[object, str]:
    categoryNames: dict[object, str] = {}
    try:
        categoryPayload = callBitrixRestMethod(
            portalUrl,
            credential,
            "crm.category.list",
            {"entityTypeId": entityTypeId},
        )
    except Exception:
        return categoryNames

    categoryResult = categoryPayload.get("result") or {}
    categories = categoryResult.get("categories") if isinstance(categoryResult, dict) else categoryResult
    for category in categories or []:
        if not isinstance(category, dict):
            continue
        categoryId = category.get("id")
        name = category.get("name") or category.get("title")
        if categoryId is None or not name:
            continue
        try:
            categoryNames[int(categoryId)] = str(name)
        except (TypeError, ValueError):
            continue
    return categoryNames


def buildBitrixCrmStageEntityIds(entityTypeId: int, categoryIds: list[int] | None = None) -> list[str]:
    uniqueCategoryIds: list[int] = []
    for value in categoryIds or []:
        try:
            categoryId = int(value or 0)
        except (TypeError, ValueError):
            continue
        if categoryId not in uniqueCategoryIds:
            uniqueCategoryIds.append(categoryId)

    entityIds: list[str] = []
    if entityTypeId == BITRIX_INVOICE_ENTITY_TYPE_ID:
        for categoryId in uniqueCategoryIds:
            entityIds.append(f"SMART_INVOICE_STAGE_{categoryId}")
            entityIds.append(f"DYNAMIC_{entityTypeId}_STAGE_{categoryId}")
        entityIds.append("SMART_INVOICE_STAGE")
    return entityIds


def fetchBitrixCrmItemDictionaries(
    portalUrl: str,
    credential: str,
    *,
    assignedByIds: list[int] | None = None,
    companyIds: list[int] | None = None,
    statusEntityIds: list[str] | None = None,
    entityTypeId: int | None = None,
    categoryIds: list[int] | None = None,
) -> dict[str, object]:
    statusNames: dict[object, str] = {}
    categoryNames: dict[object, str] = {}
    extraFieldInfo: dict[str, object] = {}
    requestedStatusEntityIds: list[str] = []
    for entityId in statusEntityIds or []:
        if entityId and entityId not in requestedStatusEntityIds:
            requestedStatusEntityIds.append(entityId)
    if entityTypeId is not None:
        categoryNames = fetchBitrixCrmCategoryNames(portalUrl, credential, entityTypeId)
        if entityTypeId == BITRIX_INVOICE_ENTITY_TYPE_ID:
            extraFieldInfo = resolveBitrixInvoiceExtraFields(portalUrl, credential)
        allCategoryIdSet = {int(value) for value in categoryNames}
        for value in categoryIds or []:
            try:
                allCategoryIdSet.add(int(value or 0))
            except (TypeError, ValueError):
                continue
        allCategoryIds = sorted(allCategoryIdSet)
        for entityId in buildBitrixCrmStageEntityIds(entityTypeId, allCategoryIds):
            if entityId and entityId not in requestedStatusEntityIds:
                requestedStatusEntityIds.append(entityId)

    for entityId in requestedStatusEntityIds:
        try:
            statusPayload = callBitrixRestMethod(
                portalUrl,
                credential,
                "crm.status.list",
                {"filter": {"ENTITY_ID": entityId}},
            )
        except Exception:
            continue
        for status in statusPayload.get("result") or []:
            statusId = status.get("STATUS_ID")
            name = status.get("NAME") or status.get("NAME_INIT") or statusId
            if statusId and name:
                statusNames[str(statusId)] = str(name)

    return {
        "status_names": statusNames,
        "category_names": categoryNames,
        "assigned_by_names": fetchBitrixUserNames(portalUrl, credential, assignedByIds or []),
        "company_names": {},
        **extraFieldInfo,
    }


def fetchBitrixProfile(portalUrl: str, credential: str) -> dict[str, object]:
    restContext = buildBitrixRestContext(portalUrl, credential, method="profile.json")
    responsePayload = callBitrixRestMethod(portalUrl, credential, "profile.json")

    resultPayload = responsePayload.get("result") or {}
    return {
        "portal_url": portalUrl.rstrip("/"),
        "auth_mode": restContext.authMode,
        "profile": resultPayload,
    }
