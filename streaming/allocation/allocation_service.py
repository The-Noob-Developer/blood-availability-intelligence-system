from inventory.inventory_repository import find_inventory
from allocation.allocation_producer import publish_allocation

def allocate_request(request):

    inventory = find_inventory(
        blood_group=request["blood_group"],
        units=request["units_required"],
        latitude=request["latitude"],
        longitude=request["longitude"]
    )

    if inventory is None:
        return False

    allocation = {
        "request_id": request["request_id"],
        "blood_bank_id": inventory["blood_bank_id"],
        "units": request["units_required"],
        "status": "ALLOCATED"
    }

    publish_allocation(allocation)

    return True