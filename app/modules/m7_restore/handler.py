from fastapi import APIRouter, Body, Request

from .service import reconcile

router = APIRouter(tags=["M7-归档恢复对账"])


@router.get("/restore")
def page(request: Request):
    data = reconcile({}, request.app.state.store, advance=False)
    return request.app.state.templates.TemplateResponse(request, "m7_restore.html", {"title": "归档恢复对账", "data": data})


@router.get("/api/restore/reconcile")
def readback(request: Request):
    """只读回读：展示已登记申请、费用次数与分区核验结果，不再提交或投递。"""
    return reconcile({}, request.app.state.store, advance=False)


@router.post("/api/restore/reconcile")
def advance(request: Request, payload: dict = Body(default={})):
    """提交或续接恢复申请；重试复用原申请，不重复计费。"""
    return reconcile(payload or {}, request.app.state.store, advance=True)
