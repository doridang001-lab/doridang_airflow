"""
gpt-oss-20b 모델 사용 예제
Hugging Face에서 모델 직접 로드 및 추론
"""

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

# ============================================
# 1. 모델 및 토크나이저 로드
# ============================================
print("모델 로드 중...")
model_name = "openai/gpt-oss-20b"

tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForCausalLM.from_pretrained(
    model_name,
    torch_dtype=torch.float16,  # GPU 메모리 절약
    device_map="auto"  # 자동으로 GPU/CPU 할당
)

print(f"✅ 모델 로드 완료: {model_name}")
print(f"Model dtype: {model.dtype}")
print(f"Device: {model.device}")

# ============================================
# 2. 추론 함수
# ============================================
def generate_text(prompt: str, max_length: int = 200):
    """
    주어진 프롬프트로 텍스트 생성
    
    Args:
        prompt: 입력 텍스트
        max_length: 최대 생성 길이
    
    Returns:
        생성된 텍스트
    """
    inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
    
    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_length=max_length,
            temperature=0.7,
            top_p=0.9,
            do_sample=True
        )
    
    generated_text = tokenizer.decode(outputs[0], skip_special_tokens=True)
    return generated_text

# ============================================
# 3. 사용 예제
# ============================================
if __name__ == "__main__":
    # 예제 1: 일반 텍스트 생성
    prompt1 = "안녕하세요. 저는"
    result1 = generate_text(prompt1)
    print(f"\n[예제 1]\nPrompt: {prompt1}\nResult: {result1}\n")
    
    # 예제 2: 안전성 정책 기반 분류
    # gpt-oss-safeguard는 안전성 평가용 모델입니다
    prompt2 = "다음 텍스트의 안전성을 평가하세요:"
    result2 = generate_text(prompt2)
    print(f"[예제 2]\nPrompt: {prompt2}\nResult: {result2}\n")
