import unittest
import importlib
import pkgutil
import nselib

class TestNselibImports(unittest.TestCase):
    def test_import_all_modules(self):
        """Recursively test that all nselib modules can be imported without errors."""
        package = nselib
        for loader, module_name, is_pkg in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
            with self.subTest(module=module_name):
                # Skip legacy/vendored or external looking modules if any, 
                # but here we want to ensure everything in nselib is healthy.
                module = importlib.import_module(module_name)
                self.assertIsNotNone(module)

if __name__ == '__main__':
    unittest.main()
