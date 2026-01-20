import pandas as pd
import hashlib

# df에 surrogate key 컬럼 추가
def add_surrogate_key(df: pd.DataFrame, natural_key_cols: list[str]) -> pd.DataFrame:
    """
    df2 = add_surrogate_key(
        df,
        natural_key_cols=["일자", "주문번호", "시간"]
    )
    df2.head()
    """
    
    # 원본 df 복사 (dtype 절대 변하지 않음)
    out = df.copy()

    # 자연키 컬럼은 key 생성에만 문자열로 변환 (df/out의 dtype은 그대로 유지)
    key_parts = [df[col].astype(str) for col in natural_key_cols]
    uk_series = pd.concat(key_parts, axis=1).agg('|'.join, axis=1)

    # Surrogate key 생성 (해시 16자리)
    out['key'] = uk_series.apply(
        lambda s: hashlib.sha1(s.encode('utf-8')).hexdigest()[:16]
    )

    # key 컬럼을 맨 앞으로 이동
    cols = ['key'] + [c for c in out.columns if c != 'key']
    out = out[cols]

    return out
