from typing import Literal

def check_threshold(
    df,
    target_col: str,
    threshold: float,
    agg_func: Literal['sum', 'mean', 'max', 'min', 'count'] = 'sum',
    condition: Literal['lt', 'le', 'gt', 'ge'] = 'lt',
    alert_message: str = None
):
    """
    범용 임계값 체크
    
    Args:
        df: 데이터프레임
        target_col: 체크할 컬럼명 (예: '주문금액', '주문건수')
        threshold: 임계값
        agg_func: 집계 함수 (sum/mean/max/min/count)
        condition: 조건 (lt=미만, le=이하, gt=초과, ge=이상)
        alert_message: 커스텀 메시지 (None이면 자동 생성)
    
    Returns:
        (is_alert_needed: bool, message: str)
    """
    if target_col not in df.columns:
        return False, f"❌ 컬럼 '{target_col}' 없음"
    
    # 집계
    agg_map = {
        'sum': df[target_col].sum(),
        'mean': df[target_col].mean(),
        'max': df[target_col].max(),
        'min': df[target_col].min(),
        'count': df[target_col].count()
    }
    value = agg_map[agg_func]
    
    # 조건 체크
    condition_map = {
        'lt': (value < threshold, '미만'),
        'le': (value <= threshold, '이하'),
        'gt': (value > threshold, '초과'),
        'ge': (value >= threshold, '이상')
    }
    is_alert, cond_text = condition_map[condition]
    
    # 메시지 생성
    if alert_message:
        message = alert_message.format(value=value, threshold=threshold)
    else:
        if is_alert:
            message = f"🚨 [{target_col}] {agg_func.upper()} {value:,.2f} {cond_text} {threshold:,.2f}"
        else:
            message = f"✅ [{target_col}] {agg_func.upper()} {value:,.2f} 정상"
    
    return is_alert, message