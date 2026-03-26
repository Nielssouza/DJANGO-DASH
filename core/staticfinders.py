import os

from django_plotly_dash.finders import DashComponentFinder


class WindowsDashComponentFinder(DashComponentFinder):
    """Dash component finder with URL/path normalization for Windows."""

    def find(self, path, find_all=False, all=False):
        all = all or find_all
        normalized_path = path.replace("\\", "/")
        matches = []

        for component_name in self.locations:
            storage = self.storages[component_name]
            location = storage.location
            component_path = f"dash/component/{component_name}".replace("\\", "/")

            if normalized_path.startswith(f"{component_path}/"):
                relative_path = normalized_path[len(component_path) + 1 :].replace("/", os.sep)
                matched_path = os.path.join(location, relative_path)

                if os.path.exists(matched_path):
                    if not all:
                        return matched_path
                    matches.append(matched_path)

        return matches
