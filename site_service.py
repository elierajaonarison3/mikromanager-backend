from mikrotik import connexion_mikrotik


def normalize_domain(domain: str) -> str:
    domain = domain.strip().lower()

    domain = domain.replace("https://", "")
    domain = domain.replace("http://", "")

    domain = domain.split("/")[0]
    domain = domain.split(":")[0]

    if domain.startswith("www."):
        domain = domain[4:]

    return domain


def block_site(domain: str):
    api = connexion_mikrotik()

    if api is None:
        return {
            "status": "error",
            "message": "Connexion MikroTik échouée"
        }

    try:
        domain = normalize_domain(domain)

        if not domain:
            return {
                "status": "error",
                "message": "Domaine invalide"
            }

        dns = api.path("ip", "dns", "static")
        firewall = api.path("ip", "firewall", "filter")

        dns_rules = list(dns)

        for rule in dns_rules:
            if rule.get("name") == domain:
                if rule.get("disabled") in ["yes", "true", True]:
                    dns.update(
                        **{
                            ".id": rule[".id"],
                            "disabled": "no"
                        }
                    )

                    return {
                        "status": "success",
                        "message": "Site débloqué puis rebloqué",
                        "domain": domain
                    }

                return {
                    "status": "success",
                    "message": "Site déjà bloqué",
                    "domain": domain
                }

        dns_rule = dns.add(
            name=domain,
            address="127.0.0.1",
            comment=f"MikroManager-BLOCK-{domain}"
        )

        return {
            "status": "success",
            "message": "Site bloqué",
            "domain": domain,
            "dns_rule_id": dns_rule.get(".id")
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "type": type(e).__name__
        }

    finally:
        api.close()


def unblock_site(domain: str):
    api = connexion_mikrotik()

    if api is None:
        return {
            "status": "error",
            "message": "Connexion MikroTik échouée"
        }

    try:
        domain = normalize_domain(domain)

        dns = api.path("ip", "dns", "static")
        rules = list(dns)

        found = None

        for rule in rules:
            if rule.get("name") == domain:
                found = rule
                break

        if found is None:
            return {
                "status": "success",
                "message": "Aucun blocage trouvé",
                "domain": domain
            }

        dns.remove(
            **{
                ".id": found[".id"]
            }
        )

        return {
            "status": "success",
            "message": "Site débloqué",
            "domain": domain
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "type": type(e).__name__
        }

    finally:
        api.close()


def get_blocked_sites():
    api = connexion_mikrotik()

    if api is None:
        return {
            "status": "error",
            "message": "Connexion MikroTik échouée"
        }

    try:
        dns = api.path("ip", "dns", "static")
        rules = list(dns)

        result = []

        for rule in rules:
            comment = rule.get("comment", "")

            if comment.startswith("MikroManager-BLOCK-"):
                result.append({
                    "id": rule.get(".id"),
                    "domain": rule.get("name"),
                    "address": rule.get("address"),
                    "disabled": rule.get("disabled"),
                    "comment": comment
                })

        return {
            "status": "success",
            "total": len(result),
            "sites": result
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "type": type(e).__name__
        }

    finally:
        api.close()