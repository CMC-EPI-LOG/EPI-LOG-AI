"""
Test script to verify the new RAG response structure
"""
import asyncio
import json
from app.services import get_medical_advice

async def debug_response_structure():
    """Test that the response includes three_reason and detail_answer fields"""
    
    # Test case: Elementary student with asthma
    test_profile = {
        "ageGroup": "elementary_high",
        "condition": "asthma"
    }
    
    station_name = "종로구"
    
    print("🧪 Testing RAG Response Structure...")
    print(f"Station: {station_name}")
    print(f"User Profile: {test_profile}\n")
    
    try:
        result = await get_medical_advice(station_name, test_profile)
        
        print("✅ Response received!")
        print("\n" + "="*60)
        print("RESPONSE STRUCTURE:")
        print("="*60)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        print("="*60)
        
        # Verify required fields
        print("\n🔍 Verification:")
        
        required_fields = ["decision", "three_reason", "detail_answer", "actionItems", "references"]
        for field in required_fields:
            if field in result:
                print(f"✅ {field}: Present")
            else:
                print(f"❌ {field}: MISSING")
        
        # Verify three_reason structure
        if "three_reason" in result:
            three_reason = result["three_reason"]
            if isinstance(three_reason, list):
                print(f"✅ three_reason is a list with {len(three_reason)} items")
                
                if len(three_reason) == 3:
                    print("✅ three_reason has exactly 3 items")
                else:
                    print(f"⚠️  three_reason has {len(three_reason)} items (expected 3)")
                
                # Check for keyword highlighting
                has_keywords = any("**" in item for item in three_reason)
                if has_keywords:
                    print("✅ Keyword highlighting (**) detected")
                    print("\n📝 three_reason items:")
                    for i, item in enumerate(three_reason, 1):
                        print(f"   {i}. {item}")
                else:
                    print("⚠️  No keyword highlighting (**) detected")
            else:
                print(f"❌ three_reason is not a list (type: {type(three_reason)})")
        
        print("\n✅ Test completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug_response_structure())
