from dataclasses import dataclass

import requests

BITRIX_DEAL_ENTITY_TYPE_ID = 2
BITRIX_PAGE_SIZE = 50
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
    "categoryId",
    "createdTime",
    "updatedTime",
]


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
    restContext = buildBitrixRestContext(portalUrl, credential)
    items: list[dict[str, object]] = []
    start = 0
    total = 0

    while True:
        payload = {
            **restContext.defaultPayload,
            "entityTypeId": BITRIX_DEAL_ENTITY_TYPE_ID,
            "select": BITRIX_DEALS_SELECT_FIELDS,
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


def fetchBitrixDealDictionaries(
    portalUrl: str,
    credential: str,
    categoryIds: list[int],
) -> dict[str, dict[object, str]]:
    stageNames: dict[object, str] = {}
    categoryNames: dict[object, str] = {0: "Общая воронка"}

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

    return {"stage_names": stageNames, "category_names": categoryNames}


def fetchBitrixProfile(portalUrl: str, credential: str) -> dict[str, object]:
    restContext = buildBitrixRestContext(portalUrl, credential, method="profile.json")
    responsePayload = callBitrixRestMethod(portalUrl, credential, "profile.json")

    resultPayload = responsePayload.get("result") or {}
    return {
        "portal_url": portalUrl.rstrip("/"),
        "auth_mode": restContext.authMode,
        "profile": resultPayload,
    }
