"""Unified verification: behavior regression, real disk comparison, structure."""
import checks
import structure

if __name__ == "__main__":
    checks.verify()
    checks.disk()
    structure.verify()
    print("VERIFY_ALL_OK")
