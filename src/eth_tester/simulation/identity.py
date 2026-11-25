"""D strategy parameter identity for new paper admissions (legacy exits exempt)."""
from ..strategies import Parameters


def rule_identity():
    params = Parameters()
    meta = params.identity()
    return f"{params.version}:{meta['parameter_sha256'][:12]}", meta


def require_new_strategy_identity(reason):
    if not isinstance(reason, dict):
        raise ValueError('New paper entry requires strategy reason mapping')
    version, meta = rule_identity()
    if (reason.get('strategy_version') != version.split(':', 1)[0]
            or reason.get('parameter_sha256') != meta['parameter_sha256']
            or reason.get('parameters') != meta['parameters']):
        raise ValueError('Unverified strategy version/parameter identity')
    return dict(reason)
